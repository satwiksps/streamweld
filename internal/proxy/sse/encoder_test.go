package sse

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestEncoderCanonicalWireFormat(t *testing.T) {
	t.Parallel()

	event := Event{
		Data:     []byte("one\n\xe2\x98\x83\n"),
		Type:     "update",
		ID:       "7",
		Retry:    1500 * time.Millisecond,
		Comments: []string{"heartbeat", ""},
		HasData:  true,
		HasType:  true,
		HasID:    true,
		HasRetry: true,
	}
	var output bytes.Buffer
	if err := NewEncoder(&output).Encode(event); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	want := ": heartbeat\n:\nevent: update\nid: 7\nretry: 1500\ndata: one\ndata: \xe2\x98\x83\ndata:\n\n"
	if got := output.String(); got != want {
		t.Fatalf("Encode() output:\n%q\nwant:\n%q", got, want)
	}

	decoded, err := NewDecoder(bytes.NewReader(output.Bytes())).Decode()
	if err != nil {
		t.Fatalf("Decode(encoded) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, event) {
		t.Fatalf("Decode(encoded) = %#v, want %#v", decoded, event)
	}
}

func TestEncoderInfersPresentNonZeroFields(t *testing.T) {
	t.Parallel()

	event := Event{
		Data:  []byte("payload"),
		Type:  "message",
		ID:    "9",
		Retry: time.Second,
	}
	var output bytes.Buffer
	if err := NewEncoder(&output).Encode(event); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := NewDecoder(bytes.NewReader(output.Bytes())).Decode()
	if err != nil {
		t.Fatalf("Decode(encoded) error = %v", err)
	}
	if !decoded.HasData || !decoded.HasType || !decoded.HasID || !decoded.HasRetry {
		t.Fatalf("Decode(encoded) presence flags = %#v", decoded)
	}
}

func TestEncoderEmptyDataField(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	if err := NewEncoder(&output).Encode(Event{HasData: true}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if got, want := output.String(), "data:\n\n"; got != want {
		t.Fatalf("Encode() output = %q, want %q", got, want)
	}
	decoded, err := NewDecoder(bytes.NewReader(output.Bytes())).Decode()
	if err != nil {
		t.Fatalf("Decode(encoded) error = %v", err)
	}
	if !decoded.HasData || decoded.Data == nil || len(decoded.Data) != 0 {
		t.Fatalf("Decode(encoded) = %#v, want present empty data", decoded)
	}
}

func TestEncoderRejectsInvalidEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		event       Event
		invalidUTF8 bool
	}{
		{name: "empty frame", event: Event{}},
		{name: "invalid UTF-8 data", event: Event{Data: []byte{0xff}}, invalidUTF8: true},
		{name: "carriage return in data", event: Event{Data: []byte("a\rb")}},
		{name: "newline in type", event: Event{Type: "a\nb"}},
		{name: "NUL in id", event: Event{ID: "a\x00b"}},
		{name: "negative retry", event: Event{Retry: -time.Millisecond, HasRetry: true}},
		{name: "fractional millisecond retry", event: Event{Retry: time.Millisecond + time.Nanosecond}},
		{name: "newline in comment", event: Event{Comments: []string{"a\nb"}}},
		{name: "invalid UTF-8 comment", event: Event{Comments: []string{string([]byte{0xff})}}, invalidUTF8: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := NewEncoder(&output).Encode(test.event)
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Encode() error = %v, want ErrInvalidEvent", err)
			}
			if got := errors.Is(err, ErrInvalidUTF8); got != test.invalidUTF8 {
				t.Fatalf("errors.Is(error, ErrInvalidUTF8) = %v, want %v; error = %v", got, test.invalidUTF8, err)
			}
			if output.Len() != 0 {
				t.Fatalf("Encode() wrote %q before validation failed", output.String())
			}
		})
	}
}

func TestEncoderHandlesShortWrites(t *testing.T) {
	t.Parallel()

	writer := &maxWriteWriter{max: 1}
	if err := NewEncoder(writer).Encode(Event{Data: []byte("payload")}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if got, want := writer.output.String(), "data: payload\n\n"; got != want {
		t.Fatalf("Encode() output = %q, want %q", got, want)
	}
}

func TestEncoderSurfacesWriterError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("write failed")
	err := NewEncoder(errorWriter{err: sentinel}).Encode(Event{Data: []byte("payload")})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Encode() error = %v, want writer error", err)
	}

	err = NewEncoder(zeroWriter{}).Encode(Event{Data: []byte("payload")})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Encode() error = %v, want io.ErrShortWrite", err)
	}
}

func TestEncoderDecoderRoundTripProperty(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewSource(0x535345))
	for iteration := 0; iteration < 2_000; iteration++ {
		event := randomEvent(random)
		var encoded bytes.Buffer
		if err := NewEncoder(&encoded).Encode(event); err != nil {
			t.Fatalf("iteration %d Encode(%#v) error = %v", iteration, event, err)
		}

		chunkSize := 1 + random.Intn(max(1, encoded.Len()))
		decoder := NewDecoder(&maxChunkReader{data: bytes.Clone(encoded.Bytes()), max: chunkSize})
		decoded, err := decoder.Decode()
		if err != nil {
			t.Fatalf("iteration %d Decode(%q) error = %v", iteration, encoded.Bytes(), err)
		}
		if !reflect.DeepEqual(decoded, event) {
			t.Fatalf("iteration %d round trip = %#v, want %#v; wire = %q", iteration, decoded, event, encoded.Bytes())
		}
		if !utf8.Valid(decoded.Data) {
			t.Fatalf("iteration %d produced invalid UTF-8 data %x", iteration, decoded.Data)
		}
		if trailing, err := decoder.Decode(); !errors.Is(err, io.EOF) {
			t.Fatalf("iteration %d trailing Decode() = (%#v, %v), want (zero, io.EOF)", iteration, trailing, err)
		} else if !reflect.DeepEqual(trailing, Event{}) {
			t.Fatalf("iteration %d trailing Decode() event = %#v, want zero", iteration, trailing)
		}
	}
}

func randomEvent(random *rand.Rand) Event {
	event := Event{}
	if random.Intn(2) == 0 {
		event.HasData = true
		event.Data = []byte(randomMultiline(random))
	}
	if random.Intn(3) == 0 {
		event.HasType = true
		event.Type = randomSingleLine(random, false)
	}
	if random.Intn(3) == 0 {
		event.HasID = true
		event.ID = randomSingleLine(random, true)
	}
	if random.Intn(3) == 0 {
		event.HasRetry = true
		event.Retry = time.Duration(random.Intn(1_000_000)) * time.Millisecond
	}
	for range random.Intn(4) {
		event.Comments = append(event.Comments, randomSingleLine(random, false))
	}
	if !event.HasData && !event.HasType && !event.HasID && !event.HasRetry && len(event.Comments) == 0 {
		event.HasData = true
		event.Data = []byte{}
	}
	return event
}

func randomMultiline(random *rand.Rand) string {
	var builder strings.Builder
	lines := 1 + random.Intn(5)
	for line := 0; line < lines; line++ {
		if line > 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(randomSingleLine(random, false))
	}
	return builder.String()
}

func randomSingleLine(random *rand.Rand, rejectNUL bool) string {
	alphabet := []rune(" abcXYZ09:[]{}\\/\t\x00\u00e9\u03bb\u2603\U0001f680")
	length := random.Intn(20)
	result := make([]rune, 0, length)
	for len(result) < length {
		r := alphabet[random.Intn(len(alphabet))]
		if rejectNUL && r == 0 {
			continue
		}
		result = append(result, r)
	}
	return string(result)
}

type maxWriteWriter struct {
	output bytes.Buffer
	max    int
}

func (w *maxWriteWriter) Write(data []byte) (int, error) {
	return w.output.Write(data[:min(len(data), w.max)])
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) {
	return 0, nil
}
