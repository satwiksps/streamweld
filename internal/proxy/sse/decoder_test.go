package sse

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
	"unicode/utf8"
)

func TestDecoderArbitraryReadBoundaries(t *testing.T) {
	t.Parallel()

	stream := []byte("\xef\xbb\xbf: heartbeat\r\nevent: update\r\nid: 42\r\nretry: 1500\r\ndata: snow \xe2\x98\x83\r\ndata: second\r\nignored: value\r\n\r\n")
	want := Event{
		Data:     []byte("snow \xe2\x98\x83\nsecond"),
		Type:     "update",
		ID:       "42",
		Retry:    1500 * time.Millisecond,
		Comments: []string{"heartbeat"},
		HasData:  true,
		HasType:  true,
		HasID:    true,
		HasRetry: true,
	}

	for maxRead := 1; maxRead <= len(stream); maxRead++ {
		maxRead := maxRead
		t.Run("max_read_"+itoa(maxRead), func(t *testing.T) {
			decoder := NewDecoder(&maxChunkReader{data: stream, max: maxRead})
			got, err := decoder.Decode()
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Decode() = %#v, want %#v", got, want)
			}
			if !utf8.Valid(got.Data) {
				t.Fatalf("Decode() split or corrupted UTF-8: %x", got.Data)
			}
			if _, err := decoder.Decode(); !errors.Is(err, io.EOF) {
				t.Fatalf("second Decode() error = %v, want io.EOF", err)
			}
		})
	}
}

func TestDecoderMultipleFramesAndEmptyData(t *testing.T) {
	t.Parallel()

	input := "\nunknown: ignored\n\n" +
		"data: first\n\n" +
		"data:\r\ndata: tail\r\n\r\n" +
		": keepalive\n\n"
	decoder := NewDecoder(bytes.NewBufferString(input))

	wants := []Event{
		{Data: []byte("first"), HasData: true},
		{Data: []byte("\ntail"), HasData: true},
		{Comments: []string{"keepalive"}},
	}
	for i, want := range wants {
		got, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Decode() frame %d error = %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Decode() frame %d = %#v, want %#v", i, got, want)
		}
	}
	if _, err := decoder.Decode(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Decode() error = %v, want io.EOF", err)
	}
}

func TestDecoderBareCRAcrossReadBoundaries(t *testing.T) {
	t.Parallel()

	stream := []byte(": heartbeat\revent: update\rid: 42\rretry: 25\rdata: alpha \xce\xbb\rdata: omega\r\r")
	want := Event{
		Data:     []byte("alpha \xce\xbb\nomega"),
		Type:     "update",
		ID:       "42",
		Retry:    25 * time.Millisecond,
		Comments: []string{"heartbeat"},
		HasData:  true,
		HasType:  true,
		HasID:    true,
		HasRetry: true,
	}

	for maxRead := 1; maxRead <= 8; maxRead++ {
		decoder := NewDecoder(&maxChunkReader{data: bytes.Clone(stream), max: maxRead})
		got, err := decoder.Decode()
		if err != nil {
			t.Fatalf("max read %d: Decode() error = %v", maxRead, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("max read %d: Decode() = %#v, want %#v", maxRead, got, want)
		}
		if _, err := decoder.Decode(); !errors.Is(err, io.EOF) {
			t.Fatalf("max read %d: second Decode() error = %v, want io.EOF", maxRead, err)
		}
	}
}

func TestDecoderFieldRules(t *testing.T) {
	t.Parallel()

	input := "event: first\n" +
		"event\n" +
		"id: before\n" +
		"id: has\x00nul\n" +
		"retry: 10\n" +
		"retry: +20\n" +
		"retry: 30\n" +
		":one\n" +
		"::two\n" +
		"data:  leading-space\n\n"

	got, err := NewDecoder(bytes.NewBufferString(input)).Decode()
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	want := Event{
		Data:     []byte(" leading-space"),
		ID:       "before",
		Retry:    30 * time.Millisecond,
		Comments: []string{"one", ":two"},
		HasData:  true,
		HasType:  true,
		HasID:    true,
		HasRetry: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Decode() = %#v, want %#v", got, want)
	}
}

func TestDecoderOnlyDispatchesOnBlankLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []byte
	}{
		{name: "unterminated line", input: []byte("data: partial")},
		{name: "terminated line without blank", input: []byte("data: partial\n")},
		{name: "comment without blank", input: []byte(": heartbeat\n")},
		{name: "unknown field without blank", input: []byte("unknown: value\n")},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := NewDecoder(bytes.NewReader(test.input)).Decode()
			if !reflect.DeepEqual(got, Event{}) {
				t.Fatalf("Decode() returned partial event %#v", got)
			}
			if !errors.Is(err, ErrIncompleteEvent) {
				t.Fatalf("Decode() error = %v, want ErrIncompleteEvent", err)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("Decode() error = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestDecoderUTF8Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         []byte
		unexpectedEOF bool
	}{
		{name: "invalid complete line", input: []byte{'d', 'a', 't', 'a', ':', ' ', 0xff, '\n', '\n'}},
		{name: "incomplete rune at EOF", input: []byte{'d', 'a', 't', 'a', ':', ' ', 0xe2, 0x82}, unexpectedEOF: true},
		{name: "invalid rune at EOF", input: []byte{'d', 'a', 't', 'a', ':', ' ', 0xff}, unexpectedEOF: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := NewDecoder(&maxChunkReader{data: test.input, max: 1}).Decode()
			if !reflect.DeepEqual(got, Event{}) {
				t.Fatalf("Decode() returned invalid event %#v", got)
			}
			if !errors.Is(err, ErrInvalidUTF8) {
				t.Fatalf("Decode() error = %v, want ErrInvalidUTF8", err)
			}
			if gotUnexpected := errors.Is(err, io.ErrUnexpectedEOF); gotUnexpected != test.unexpectedEOF {
				t.Fatalf("errors.Is(error, io.ErrUnexpectedEOF) = %v, want %v; error = %v", gotUnexpected, test.unexpectedEOF, err)
			}
		})
	}
}

func TestDecoderSurfacesReaderError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("source failed")
	decoder := NewDecoder(&errorReader{data: []byte("data: partial"), err: sentinel})
	_, err := decoder.Decode()
	if !errors.Is(err, ErrIncompleteEvent) {
		t.Fatalf("Decode() error = %v, want ErrIncompleteEvent", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("Decode() error = %v, want source error", err)
	}
}

func TestDecoderEOFBetweenFrames(t *testing.T) {
	t.Parallel()

	decoder := NewDecoder(bytes.NewBufferString("data: complete\n\n"))
	got, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if string(got.Data) != "complete" || !got.HasData {
		t.Fatalf("Decode() = %#v", got)
	}
	if _, err := decoder.Decode(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Decode() error = %v, want io.EOF", err)
	}
}

func TestDecoderOptions(t *testing.T) {
	t.Parallel()

	decoder, err := NewDecoderWithOptions(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	if decoder.maxEventBytes != DefaultMaxEventBytes {
		t.Fatalf("default maximum = %d, want %d", decoder.maxEventBytes, DefaultMaxEventBytes)
	}

	decoder, err = NewDecoderWithOptions(bytes.NewReader(nil), WithMaxEventBytes(17))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions(custom limit) error = %v", err)
	}
	if decoder.maxEventBytes != 17 {
		t.Fatalf("custom maximum = %d, want 17", decoder.maxEventBytes)
	}

	for _, test := range []struct {
		name   string
		option DecoderOption
	}{
		{name: "zero", option: WithMaxEventBytes(0)},
		{name: "negative", option: WithMaxEventBytes(-1)},
		{name: "nil option", option: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewDecoderWithOptions(bytes.NewReader(nil), test.option); err == nil {
				t.Fatal("NewDecoderWithOptions() accepted an invalid option")
			}
		})
	}
}

func TestDecoderMaxEventBytesSingleLineBoundary(t *testing.T) {
	t.Parallel()

	const line = "data: x"
	input := []byte(line + "\r\n\r\n")
	decoder, err := NewDecoderWithOptions(
		&maxChunkReader{data: bytes.Clone(input), max: 1},
		WithMaxEventBytes(len(line)),
	)
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	event, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode() at exact limit error = %v", err)
	}
	if got := string(event.Data); got != "x" || !event.HasData {
		t.Fatalf("Decode() at exact limit = %#v", event)
	}
	if _, err := decoder.Decode(); !errors.Is(err, io.EOF) {
		t.Fatalf("Decode() after exact-limit frame error = %v, want io.EOF", err)
	}

	decoder, err = NewDecoderWithOptions(bytes.NewReader(input), WithMaxEventBytes(len(line)-1))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	if _, err := decoder.Decode(); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("Decode() one byte over limit error = %v, want ErrEventTooLarge", err)
	}
}

func TestDecoderMaxEventBytesAggregatesLines(t *testing.T) {
	t.Parallel()

	const firstLine = "data:a"
	const secondLine = "data:b"
	input := []byte(firstLine + "\n" + secondLine + "\n\n")
	limit := len(firstLine) + len(secondLine)

	decoder, err := NewDecoderWithOptions(bytes.NewReader(input), WithMaxEventBytes(limit))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	event, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode() at aggregate limit error = %v", err)
	}
	if got, want := string(event.Data), "a\nb"; got != want {
		t.Fatalf("Decode() data = %q, want %q", got, want)
	}

	decoder, err = NewDecoderWithOptions(bytes.NewReader(input), WithMaxEventBytes(limit-1))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	if _, err := decoder.Decode(); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("Decode() aggregate overflow error = %v, want ErrEventTooLarge", err)
	}
}

func TestDecoderMaxEventBytesRejectsDelimiterFreeLine(t *testing.T) {
	t.Parallel()

	const limit = 32
	exact := bytes.Repeat([]byte{'x'}, limit)
	decoder, err := NewDecoderWithOptions(bytes.NewReader(exact), WithMaxEventBytes(limit))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	if _, err := decoder.Decode(); !errors.Is(err, ErrIncompleteEvent) || errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("Decode() exact unterminated line error = %v, want only ErrIncompleteEvent", err)
	}

	overLimit := append(bytes.Clone(exact), 'x')
	decoder, err = NewDecoderWithOptions(
		&maxChunkReader{data: overLimit, max: 1},
		WithMaxEventBytes(limit),
	)
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	if _, err := decoder.Decode(); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("Decode() delimiter-free overflow error = %v, want ErrEventTooLarge", err)
	}
}

func TestDecoderMaxEventBytesResetsAtEveryBlankFrame(t *testing.T) {
	t.Parallel()

	const line = "data:a"
	decoder, err := NewDecoderWithOptions(
		bytes.NewBufferString(line+"\n\n"+line+"\n\n"),
		WithMaxEventBytes(len(line)),
	)
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	for frame := 0; frame < 2; frame++ {
		if _, err := decoder.Decode(); err != nil {
			t.Fatalf("Decode() frame %d error = %v", frame, err)
		}
	}
	if _, err := decoder.Decode(); !errors.Is(err, io.EOF) {
		t.Fatalf("Decode() after two frames error = %v, want io.EOF", err)
	}

	// Unknown-only frames are skipped, but their terminating blank line must
	// still reset the byte count before the next recognized frame.
	const ignoredLine = "other:"
	decoder, err = NewDecoderWithOptions(
		bytes.NewBufferString(ignoredLine+"\n\n"+line+"\n\n"),
		WithMaxEventBytes(len(line)),
	)
	if err != nil {
		t.Fatalf("NewDecoderWithOptions() error = %v", err)
	}
	if _, err := decoder.Decode(); err != nil {
		t.Fatalf("Decode() after skipped frame error = %v", err)
	}
}

type maxChunkReader struct {
	data []byte
	max  int
}

func (r *maxChunkReader) Read(dst []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(r.data), len(dst), r.max)
	copy(dst, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

type errorReader struct {
	data []byte
	err  error
}

func (r *errorReader) Read(dst []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(dst, r.data)
	r.data = r.data[n:]
	return n, r.err
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	pos := len(digits)
	for value > 0 {
		pos--
		digits[pos] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[pos:])
}
