package sse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

// DefaultMaxEventBytes is the maximum number of non-delimiter wire bytes that
// NewDecoder accepts in one frame. It prevents an unterminated or exceptionally
// large upstream frame from growing memory without bound.
const DefaultMaxEventBytes = 1 << 20

// DecoderOption configures a Decoder created by NewDecoderWithOptions.
type DecoderOption func(*Decoder) error

// WithMaxEventBytes sets the maximum number of non-delimiter wire bytes in one
// frame. The count includes every field and comment line, including unknown
// fields, but excludes CR and LF line delimiters. The value must be positive.
func WithMaxEventBytes(maxBytes int) DecoderOption {
	return func(decoder *Decoder) error {
		if maxBytes <= 0 {
			return fmt.Errorf("sse: maximum event bytes must be positive, got %d", maxBytes)
		}
		decoder.maxEventBytes = maxBytes
		return nil
	}
}

// Decoder incrementally reads complete SSE frames from an io.Reader.
//
// Input may be divided at arbitrary read boundaries, including in the middle
// of CRLF pairs or UTF-8 encodings. CR, LF, and CRLF line endings are accepted.
// NewDecoder applies DefaultMaxEventBytes. Use NewDecoderWithOptions to select
// a different positive limit.
type Decoder struct {
	reader        *bufio.Reader
	frame         Event
	recognized    bool
	blockStarted  bool
	firstLine     bool
	skipLF        bool
	offset        int64
	frameBytes    int
	maxEventBytes int
}

// NewDecoder returns a decoder reading from r.
func NewDecoder(r io.Reader) *Decoder {
	return newDecoder(r)
}

// NewDecoderWithOptions returns a configured decoder reading from r. It
// validates every option before any input is consumed.
func NewDecoderWithOptions(r io.Reader, options ...DecoderOption) (*Decoder, error) {
	decoder := newDecoder(r)
	for _, option := range options {
		if option == nil {
			return nil, errors.New("sse: decoder option cannot be nil")
		}
		if err := option(decoder); err != nil {
			return nil, fmt.Errorf("sse: apply decoder option: %w", err)
		}
	}
	return decoder, nil
}

func newDecoder(r io.Reader) *Decoder {
	return &Decoder{
		reader:        bufio.NewReader(r),
		firstLine:     true,
		maxEventBytes: DefaultMaxEventBytes,
	}
}

// Decode returns the next complete, recognized frame.
//
// Blank frames and frames containing only unknown fields are skipped. Comment-
// only and control-field-only frames are returned so a proxy can faithfully
// re-emit keepalives and reconnection metadata. Decode returns io.EOF only
// between frames. EOF within a frame returns an error matching both
// ErrIncompleteEvent and io.ErrUnexpectedEOF.
func (d *Decoder) Decode() (Event, error) {
	for {
		line, err := d.readLine()
		if err != nil {
			return Event{}, err
		}

		if len(line) == 0 {
			event, ok := d.finishFrame()
			if ok {
				return event, nil
			}
			continue
		}

		d.blockStarted = true
		d.consumeLine(line)
	}
}

func (d *Decoder) readLine() ([]byte, error) {
	lineStart := d.offset
	line := make([]byte, 0, 128)

	for {
		b, err := d.reader.ReadByte()
		if err != nil {
			line = d.stripInitialBOM(line)
			if len(line) > 0 && !utf8.Valid(line) {
				if errors.Is(err, io.EOF) {
					return nil, fmt.Errorf("%w at byte %d: %w", ErrInvalidUTF8, lineStart, io.ErrUnexpectedEOF)
				}
				return nil, fmt.Errorf("%w at byte %d: %w", ErrInvalidUTF8, lineStart, err)
			}

			if errors.Is(err, io.EOF) {
				if len(line) == 0 && !d.blockStarted {
					return nil, io.EOF
				}
				return nil, fmt.Errorf("%w at byte %d: %w", ErrIncompleteEvent, lineStart, io.ErrUnexpectedEOF)
			}

			if len(line) > 0 || d.blockStarted {
				return nil, fmt.Errorf("%w at byte %d: %w", ErrIncompleteEvent, lineStart, err)
			}
			return nil, err
		}
		d.offset++

		// A CR already terminated the previous line. Consume exactly one LF as
		// the second half of CRLF; any other byte begins the next line.
		if d.skipLF {
			d.skipLF = false
			lineStart = d.offset - 1
			if b == '\n' {
				lineStart = d.offset
				continue
			}
		}

		switch b {
		case '\r':
			d.skipLF = true
			return d.completeLine(line, lineStart)
		case '\n':
			return d.completeLine(line, lineStart)
		default:
			if d.frameBytes >= d.maxEventBytes {
				return nil, fmt.Errorf("%w: limit %d bytes at byte %d", ErrEventTooLarge, d.maxEventBytes, d.offset-1)
			}
			d.frameBytes++
			line = append(line, b)
		}
	}
}

func (d *Decoder) completeLine(line []byte, lineStart int64) ([]byte, error) {
	line = d.stripInitialBOM(line)
	if !utf8.Valid(line) {
		return nil, fmt.Errorf("%w at byte %d", ErrInvalidUTF8, lineStart)
	}
	return line, nil
}

func (d *Decoder) stripInitialBOM(line []byte) []byte {
	if !d.firstLine {
		return line
	}
	d.firstLine = false
	return bytes.TrimPrefix(line, utf8BOM)
}

func (d *Decoder) consumeLine(line []byte) {
	colon := bytes.IndexByte(line, ':')
	if colon == 0 {
		d.frame.Comments = append(d.frame.Comments, string(fieldValue(line[1:])))
		d.recognized = true
		return
	}

	var name, value []byte
	if colon < 0 {
		name = line
	} else {
		name = line[:colon]
		value = fieldValue(line[colon+1:])
	}

	switch string(name) {
	case "data":
		d.frame.Data = append(d.frame.Data, value...)
		d.frame.Data = append(d.frame.Data, '\n')
		d.frame.HasData = true
		d.recognized = true
	case "event":
		d.frame.Type = string(value)
		d.frame.HasType = true
		d.recognized = true
	case "id":
		// The EventSource algorithm ignores an id field containing NUL.
		if !bytes.ContainsRune(value, '\x00') {
			d.frame.ID = string(value)
			d.frame.HasID = true
			d.recognized = true
		}
	case "retry":
		if retry, ok := parseRetry(value); ok {
			d.frame.Retry = retry
			d.frame.HasRetry = true
			d.recognized = true
		}
	}
}

func fieldValue(value []byte) []byte {
	if len(value) > 0 && value[0] == ' ' {
		return value[1:]
	}
	return value
}

func parseRetry(value []byte) (time.Duration, bool) {
	if len(value) == 0 {
		return 0, false
	}
	for _, b := range value {
		if b < '0' || b > '9' {
			return 0, false
		}
	}
	milliseconds, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil || milliseconds > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return 0, false
	}
	return time.Duration(milliseconds) * time.Millisecond, true
}

func (d *Decoder) finishFrame() (Event, bool) {
	event := d.frame
	ok := d.recognized
	if event.HasData {
		// Every data field contributed exactly one trailing LF.
		event.Data = event.Data[:len(event.Data)-1]
	}

	d.frame = Event{}
	d.recognized = false
	d.blockStarted = false
	d.frameBytes = 0
	return event, ok
}
