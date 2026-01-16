package sse

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"
)

// Encoder writes complete SSE frames to an io.Writer. It uses canonical LF
// line endings and a single space after non-empty field separators. Encoding
// preserves Event semantics, but not the original wire bytes or field order.
type Encoder struct {
	writer io.Writer
}

// NewEncoder returns an encoder writing to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{writer: w}
}

// Encode validates and writes one complete event, including its terminating
// blank line. Data containing LF is emitted as multiple data fields.
func (e *Encoder) Encode(event Event) error {
	hasData := event.HasData || event.Data != nil
	hasType := event.HasType || event.Type != ""
	hasID := event.HasID || event.ID != ""
	hasRetry := event.HasRetry || event.Retry != 0

	if !hasData && !hasType && !hasID && !hasRetry && len(event.Comments) == 0 {
		return fmt.Errorf("%w: frame has no recognized fields", ErrInvalidEvent)
	}
	if err := validateEvent(event, hasData, hasType, hasID, hasRetry); err != nil {
		return err
	}

	var frame bytes.Buffer
	for _, comment := range event.Comments {
		writeField(&frame, "", []byte(comment))
	}
	if hasType {
		writeField(&frame, "event", []byte(event.Type))
	}
	if hasID {
		writeField(&frame, "id", []byte(event.ID))
	}
	if hasRetry {
		milliseconds := event.Retry / time.Millisecond
		writeField(&frame, "retry", strconv.AppendInt(nil, int64(milliseconds), 10))
	}
	if hasData {
		for _, line := range bytes.Split(event.Data, []byte{'\n'}) {
			writeField(&frame, "data", line)
		}
	}
	frame.WriteByte('\n')

	return writeAll(e.writer, frame.Bytes())
}

func validateEvent(event Event, hasData, hasType, hasID, hasRetry bool) error {
	if hasData {
		if !utf8.Valid(event.Data) {
			return fmt.Errorf("%w: data: %w", ErrInvalidEvent, ErrInvalidUTF8)
		}
		if bytes.IndexByte(event.Data, '\r') >= 0 {
			return fmt.Errorf("%w: data contains a carriage return", ErrInvalidEvent)
		}
	}
	if hasType {
		if err := validateSingleLine("event", event.Type, false); err != nil {
			return err
		}
	}
	if hasID {
		if err := validateSingleLine("id", event.ID, true); err != nil {
			return err
		}
	}
	if hasRetry {
		if event.Retry < 0 {
			return fmt.Errorf("%w: retry duration is negative", ErrInvalidEvent)
		}
		if event.Retry%time.Millisecond != 0 {
			return fmt.Errorf("%w: retry duration is not a whole millisecond", ErrInvalidEvent)
		}
	}
	for _, comment := range event.Comments {
		if err := validateSingleLine("comment", comment, false); err != nil {
			return err
		}
	}
	return nil
}

func validateSingleLine(field, value string, rejectNUL bool) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s: %w", ErrInvalidEvent, field, ErrInvalidUTF8)
	}
	if bytes.ContainsAny([]byte(value), "\r\n") {
		return fmt.Errorf("%w: %s contains a line ending", ErrInvalidEvent, field)
	}
	if rejectNUL && bytes.IndexByte([]byte(value), 0) >= 0 {
		return fmt.Errorf("%w: %s contains NUL", ErrInvalidEvent, field)
	}
	return nil
}

func writeField(dst *bytes.Buffer, name string, value []byte) {
	if name != "" {
		dst.WriteString(name)
	}
	dst.WriteByte(':')
	if len(value) > 0 {
		dst.WriteByte(' ')
		dst.Write(value)
	}
	dst.WriteByte('\n')
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
