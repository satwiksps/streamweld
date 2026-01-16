package sse

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"unicode/utf8"
)

// FuzzDecoder uses an unchunked decode as a semantic oracle for two different
// read boundaries. It compares every frame, not just the first one, and repeats
// the comparison with a small configured frame limit to exercise overflow.
func FuzzDecoder(f *testing.F) {
	seeds := [][]byte{
		{},
		[]byte("\n"),
		[]byte("data: hello\n\n"),
		[]byte("data: first\r\ndata: second\r\n\r\n"),
		[]byte(": bare CR\revent: token\rid: 18\rretry: 500\rdata: first\rdata: second\r\r"),
		[]byte(": heartbeat\nevent: token\nid: 17\nretry: 250\ndata: \xe2\x98\x83\n\n"),
		[]byte("unknown: ignored\ndata:\n\n"),
		[]byte("data: one\n\n: heartbeat\n\nunknown: skipped\n\nid: 2\ndata: two\r\n\r\ndata: three\r\r"),
		[]byte("data: 0123456789abcdefghijklmnopqrstuvwxyz\n\n"),
		[]byte("data: unterminated"),
		{'d', 'a', 't', 'a', ':', ' ', 0xe2, 0x82},
		{'d', 'a', 't', 'a', ':', ' ', 0xff, '\n', '\n'},
		[]byte("\xef\xbb\xbfdata: bom\n\n"),
	}
	for _, seed := range seeds {
		for _, maxRead := range []byte{1, 2, 7, 64} {
			f.Add(seed, maxRead)
		}
	}

	f.Fuzz(func(t *testing.T, input []byte, boundarySeed byte) {
		firstChunk := int(boundarySeed%64) + 1
		secondChunk := (int(boundarySeed)*37+11)%64 + 1

		assertChunkEquivalent(t, input, DefaultMaxEventBytes, firstChunk, secondChunk)
		assertChunkEquivalent(t, input, int(boundarySeed%128)+1, firstChunk, secondChunk)
	})
}

func assertChunkEquivalent(t *testing.T, input []byte, limit int, chunkSizes ...int) {
	t.Helper()
	wantEvents, wantTerminal := decodeForFuzz(t, bytes.NewReader(input), len(input), limit)
	for _, chunkSize := range chunkSizes {
		gotEvents, gotTerminal := decodeForFuzz(t, &maxChunkReader{
			data: bytes.Clone(input),
			max:  chunkSize,
		}, len(input), limit)
		if !reflect.DeepEqual(gotEvents, wantEvents) || gotTerminal != wantTerminal {
			t.Fatalf(
				"chunk size %d changed decode semantics at limit %d:\n got events %#v, terminal %s\nwant events %#v, terminal %s\ninput %x",
				chunkSize,
				limit,
				gotEvents,
				gotTerminal,
				wantEvents,
				wantTerminal,
				input,
			)
		}
	}
}

func decodeForFuzz(t *testing.T, reader io.Reader, inputLength, limit int) ([]Event, string) {
	t.Helper()
	decoder, err := NewDecoderWithOptions(reader, WithMaxEventBytes(limit))
	if err != nil {
		t.Fatalf("NewDecoderWithOptions(limit %d) error = %v", limit, err)
	}

	events := make([]Event, 0)
	for frameCount := 0; ; frameCount++ {
		// A finite input cannot produce more recognized frames than bytes. This
		// turns an accidental no-progress loop into a clear failure.
		if frameCount > inputLength+1 {
			t.Fatalf("decoder made no progress after %d frames from %d input bytes", frameCount, inputLength)
		}

		event, err := decoder.Decode()
		if err != nil {
			return events, decoderTerminal(t, err)
		}
		assertValidUTF8Event(t, event)
		events = append(events, event)
	}
}

func decoderTerminal(t *testing.T, err error) string {
	t.Helper()
	switch {
	case errors.Is(err, ErrEventTooLarge):
		return "too_large"
	case errors.Is(err, ErrInvalidUTF8):
		return "invalid_utf8"
	case errors.Is(err, ErrIncompleteEvent):
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("EOF truncation did not wrap io.ErrUnexpectedEOF: %v", err)
		}
		return "incomplete"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		t.Fatalf("Decode() returned unexpected error: %v", err)
		return "unreachable"
	}
}

func assertValidUTF8Event(t *testing.T, event Event) {
	t.Helper()
	if event.HasData && !utf8.Valid(event.Data) {
		t.Fatalf("Decode() returned split or invalid UTF-8 data: %x", event.Data)
	}
	if event.HasType && !utf8.ValidString(event.Type) {
		t.Fatalf("Decode() returned invalid UTF-8 event type: %x", event.Type)
	}
	if event.HasID && !utf8.ValidString(event.ID) {
		t.Fatalf("Decode() returned invalid UTF-8 id: %x", event.ID)
	}
	for _, comment := range event.Comments {
		if !utf8.ValidString(comment) {
			t.Fatalf("Decode() returned invalid UTF-8 comment: %x", comment)
		}
	}
}
