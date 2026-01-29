package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestJournalSSEWriterMapsEveryJournalKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		entry      journal.Entry
		wantResult SSEWriteResult
		wantWire   string
	}{
		{
			name:       "open",
			entry:      journal.Entry{Seq: 1, Kind: journal.KindOpen, Payload: json.RawMessage(`{"stream_id":"01abc","model":"m"}`)},
			wantResult: SSEWriteResult{Visible: true},
			wantWire:   "event: streamweld.stream.open\nid: 1\ndata: {\"stream_id\":\"01abc\",\"model\":\"m\"}\n\n",
		},
		{
			name:       "chunk",
			entry:      journal.Entry{Seq: 2, Kind: journal.KindChunk, Payload: json.RawMessage(`{"data":"hello","upstream_event":"vendor"}`)},
			wantResult: SSEWriteResult{Visible: true},
			wantWire:   "id: 2\ndata: hello\n\n",
		},
		{
			name:       "migration",
			entry:      journal.Entry{Seq: 3, Kind: journal.KindMigration, Payload: json.RawMessage(`{"from_backend":"a","to_backend":"b"}`)},
			wantResult: SSEWriteResult{Visible: true},
			wantWire:   "event: streamweld.stream.migration\nid: 3\ndata: {\"from_backend\":\"a\",\"to_backend\":\"b\"}\n\n",
		},
		{
			name:       "warning",
			entry:      journal.Entry{Seq: 4, Kind: journal.KindWarning, Payload: json.RawMessage(`{"code":"seam_anomaly"}`)},
			wantResult: SSEWriteResult{Visible: true},
			wantWire:   "event: streamweld.stream.warning\nid: 4\ndata: {\"code\":\"seam_anomaly\"}\n\n",
		},
		{
			name:       "error",
			entry:      journal.Entry{Seq: 5, Kind: journal.KindError, Payload: json.RawMessage(`{"code":"upstream_error"}`)},
			wantResult: SSEWriteResult{Visible: true, Terminal: true},
			wantWire:   "event: streamweld.stream.error\nid: 5\ndata: {\"code\":\"upstream_error\"}\n\n",
		},
		{
			name:       "done",
			entry:      journal.Entry{Seq: 6, Kind: journal.KindDone, Payload: json.RawMessage(`{"finish_reason":"stop"}`)},
			wantResult: SSEWriteResult{Visible: true, Terminal: true},
			wantWire:   "event: streamweld.stream.done\nid: 6\ndata: {\"finish_reason\":\"stop\"}\n\ndata: [DONE]\n\n",
		},
		{
			name:       "stopped",
			entry:      journal.Entry{Seq: 7, Kind: journal.KindStopped, Payload: json.RawMessage(`{"usage":{"total_tokens":3}}`)},
			wantResult: SSEWriteResult{Visible: true, Terminal: true},
			wantWire:   "event: streamweld.stream.stopped\nid: 7\ndata: {\"usage\":{\"total_tokens\":3}}\n\n",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var output bytes.Buffer
			writer := NewJournalSSEWriter(&output, true)
			result, err := writer.WriteEntry(test.entry)
			if err != nil {
				t.Fatalf("WriteEntry() error = %v", err)
			}
			if !reflect.DeepEqual(result, test.wantResult) {
				t.Fatalf("WriteEntry() result = %#v, want %#v", result, test.wantResult)
			}
			if got := output.String(); got != test.wantWire {
				t.Fatalf("WriteEntry() wire = %q, want %q", got, test.wantWire)
			}
		})
	}
}

func TestJournalSSEWriterFiltersVerboseEntriesWithoutRenumbering(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	writer := NewJournalSSEWriter(&output, false)
	entries := []journal.Entry{
		{Seq: 1, Kind: journal.KindOpen, Payload: json.RawMessage(`{"stream_id":"01abc"}`)},
		{Seq: 2, Kind: journal.KindMigration, Payload: json.RawMessage(`{"attempt":2}`)},
		{Seq: 3, Kind: journal.KindWarning, Payload: json.RawMessage(`{"code":"token_count_estimated"}`)},
		{Seq: 4, Kind: journal.KindChunk, Payload: json.RawMessage(`{"data":"next"}`)},
	}

	for index, entry := range entries {
		result, err := writer.WriteEntry(entry)
		if err != nil {
			t.Fatalf("WriteEntry(entries[%d]) error = %v", index, err)
		}
		wantVisible := entry.Kind == journal.KindOpen || entry.Kind == journal.KindChunk
		if result.Visible != wantVisible || result.Terminal {
			t.Fatalf("WriteEntry(entries[%d]) result = %#v, want Visible %v and non-terminal", index, result, wantVisible)
		}
	}

	want := "event: streamweld.stream.open\nid: 1\ndata: {\"stream_id\":\"01abc\"}\n\nid: 4\ndata: next\n\n"
	if got := output.String(); got != want {
		t.Fatalf("filtered wire = %q, want %q", got, want)
	}
}

func TestJournalSSEWriterPreservesPayloadSemantics(t *testing.T) {
	t.Parallel()

	t.Run("control JSON bytes", func(t *testing.T) {
		t.Parallel()

		payload := json.RawMessage(" {\n  \"code\": \"seam_anomaly\",\n  \"details\": {}\n} ")
		var output bytes.Buffer
		writer := NewJournalSSEWriter(&output, true)
		if _, err := writer.WriteEntry(journal.Entry{Seq: 9, Kind: journal.KindWarning, Payload: payload}); err != nil {
			t.Fatalf("WriteEntry() error = %v", err)
		}

		events := decodeJournalSSE(t, output.Bytes())
		if len(events) != 1 {
			t.Fatalf("decoded event count = %d, want 1", len(events))
		}
		if got := events[0].Data; !bytes.Equal(got, payload) {
			t.Fatalf("decoded data = %q, want exact payload %q", got, payload)
		}
	})

	t.Run("multiline chunk", func(t *testing.T) {
		t.Parallel()

		const data = "first\n\n雪\n"
		payload, err := json.Marshal(map[string]any{"data": data, "upstream_event": "ignored", "vendor": 7})
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		var output bytes.Buffer
		writer := NewJournalSSEWriter(&output, false)
		if _, err := writer.WriteEntry(journal.Entry{Seq: 10, Kind: journal.KindChunk, Payload: payload}); err != nil {
			t.Fatalf("WriteEntry() error = %v", err)
		}

		wantWire := "id: 10\ndata: first\ndata:\ndata: 雪\ndata:\n\n"
		if got := output.String(); got != wantWire {
			t.Fatalf("multiline chunk wire = %q, want %q", got, wantWire)
		}
		events := decodeJournalSSE(t, output.Bytes())
		if len(events) != 1 || string(events[0].Data) != data {
			t.Fatalf("decoded events = %#v, want one event with data %q", events, data)
		}
		if events[0].HasType {
			t.Fatalf("chunk event unexpectedly has event type %q", events[0].Type)
		}
	})
}

func TestJournalSSEWriterTransportHelpersAreUnsequenced(t *testing.T) {
	t.Parallel()

	t.Run("done sentinel", func(t *testing.T) {
		t.Parallel()

		var output bytes.Buffer
		writer := NewJournalSSEWriter(&output, false)
		if err := writer.WriteDoneSentinel(); err != nil {
			t.Fatalf("WriteDoneSentinel() error = %v", err)
		}
		if got, want := output.String(), "data: [DONE]\n\n"; got != want {
			t.Fatalf("WriteDoneSentinel() wire = %q, want %q", got, want)
		}
		events := decodeJournalSSE(t, output.Bytes())
		if len(events) != 1 || events[0].HasID || events[0].HasType || string(events[0].Data) != doneSentinelData {
			t.Fatalf("decoded done sentinel = %#v", events)
		}
	})

	t.Run("reader lag", func(t *testing.T) {
		t.Parallel()

		var output bytes.Buffer
		writer := NewJournalSSEWriter(&output, false)
		if err := writer.WriteReaderLagError(); err != nil {
			t.Fatalf("WriteReaderLagError() error = %v", err)
		}
		wantWire := "event: streamweld.reader.error\ndata: {\"code\":\"reader_lag_exceeded\"}\n\n"
		if got := output.String(); got != wantWire {
			t.Fatalf("WriteReaderLagError() wire = %q, want %q", got, wantWire)
		}
		events := decodeJournalSSE(t, output.Bytes())
		if len(events) != 1 || events[0].HasID || events[0].Type != readerErrorEvent || string(events[0].Data) != readerLagErrorData {
			t.Fatalf("decoded reader lag event = %#v", events)
		}
	})
}

func TestJournalSSEWriterRejectsMalformedPayloadsWithoutWriting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kind    journal.EntryKind
		payload json.RawMessage
	}{
		{name: "nil", kind: journal.KindOpen},
		{name: "empty", kind: journal.KindOpen, payload: json.RawMessage{}},
		{name: "invalid JSON", kind: journal.KindOpen, payload: json.RawMessage(`{"x":`)},
		{name: "null", kind: journal.KindOpen, payload: json.RawMessage(`null`)},
		{name: "array", kind: journal.KindOpen, payload: json.RawMessage(`[]`)},
		{name: "string", kind: journal.KindOpen, payload: json.RawMessage(`"value"`)},
		{name: "invalid UTF-8", kind: journal.KindOpen, payload: json.RawMessage{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}},
		{name: "chunk missing data", kind: journal.KindChunk, payload: json.RawMessage(`{"upstream_event":null}`)},
		{name: "chunk null data", kind: journal.KindChunk, payload: json.RawMessage(`{"data":null}`)},
		{name: "chunk numeric data", kind: journal.KindChunk, payload: json.RawMessage(`{"data":1}`)},
		{name: "chunk boolean data", kind: journal.KindChunk, payload: json.RawMessage(`{"data":true}`)},
		{name: "chunk object data", kind: journal.KindChunk, payload: json.RawMessage(`{"data":{}}`)},
		{name: "chunk array data", kind: journal.KindChunk, payload: json.RawMessage(`{"data":[]}`)},
		{name: "filtered migration is still validated", kind: journal.KindMigration, payload: json.RawMessage(`{"attempt":`)},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var output bytes.Buffer
			writer := NewJournalSSEWriter(&output, false)
			result, err := writer.WriteEntry(journal.Entry{Seq: 1, Kind: test.kind, Payload: test.payload})
			if !errors.Is(err, ErrMalformedJournalPayload) {
				t.Fatalf("WriteEntry() error = %v, want ErrMalformedJournalPayload", err)
			}
			if result != (SSEWriteResult{}) {
				t.Fatalf("WriteEntry() result = %#v, want zero", result)
			}
			if output.Len() != 0 {
				t.Fatalf("WriteEntry() wrote %q before rejecting payload", output.String())
			}
		})
	}
}

func TestJournalSSEWriterRejectsInvalidEnvelopesWithoutWriting(t *testing.T) {
	t.Parallel()

	deliveryError := errors.New("tail failed")
	tests := []struct {
		name  string
		entry journal.Entry
	}{
		{
			name:  "zero sequence",
			entry: journal.Entry{Kind: journal.KindOpen, Payload: json.RawMessage(`{}`)},
		},
		{
			name:  "delivery error",
			entry: journal.Entry{Seq: 1, Kind: journal.KindOpen, Payload: json.RawMessage(`{}`), Err: deliveryError},
		},
		{
			name:  "unknown kind",
			entry: journal.Entry{Seq: 1, Kind: journal.EntryKind("vendor"), Payload: json.RawMessage(`{}`)},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var output bytes.Buffer
			writer := NewJournalSSEWriter(&output, true)
			result, err := writer.WriteEntry(test.entry)
			if !errors.Is(err, ErrInvalidJournalEntry) {
				t.Fatalf("WriteEntry() error = %v, want ErrInvalidJournalEntry", err)
			}
			if result != (SSEWriteResult{}) || output.Len() != 0 {
				t.Fatalf("WriteEntry() = (%#v, wire %q), want zero result and wire", result, output.String())
			}
		})
	}
}

func TestJournalSSEWriterRejectsUnencodablePayloadWithoutPartialFrame(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage("{\r\n\"code\":\"upstream_error\"\r\n}")
	var output bytes.Buffer
	writer := NewJournalSSEWriter(&output, false)
	_, err := writer.WriteEntry(journal.Entry{Seq: 1, Kind: journal.KindError, Payload: payload})
	if !errors.Is(err, sse.ErrInvalidEvent) {
		t.Fatalf("WriteEntry() error = %v, want sse.ErrInvalidEvent", err)
	}
	if output.Len() != 0 {
		t.Fatalf("WriteEntry() wrote partial frame %q", output.String())
	}
}

func TestJournalSSEWriterSurfacesUnderlyingWriteErrors(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("socket closed")
	t.Run("entry", func(t *testing.T) {
		t.Parallel()

		output := &failOnWrite{failAt: 1, err: sentinel}
		writer := NewJournalSSEWriter(output, false)
		_, err := writer.WriteEntry(journal.Entry{Seq: 1, Kind: journal.KindOpen, Payload: json.RawMessage(`{}`)})
		if !errors.Is(err, sentinel) {
			t.Fatalf("WriteEntry() error = %v, want socket error", err)
		}
	})

	t.Run("done sentinel after terminal frame", func(t *testing.T) {
		t.Parallel()

		output := &failOnWrite{failAt: 2, err: sentinel}
		writer := NewJournalSSEWriter(output, false)
		result, err := writer.WriteEntry(journal.Entry{Seq: 4, Kind: journal.KindDone, Payload: json.RawMessage(`{}`)})
		if !errors.Is(err, sentinel) {
			t.Fatalf("WriteEntry() error = %v, want socket error", err)
		}
		if result != (SSEWriteResult{}) {
			t.Fatalf("WriteEntry() result = %#v, want zero on error", result)
		}
		want := "event: streamweld.stream.done\nid: 4\ndata: {}\n\n"
		if got := output.output.String(); got != want {
			t.Fatalf("successful first frame = %q, want %q", got, want)
		}
	})
}

func TestJournalSSEWriterRequiresInitializedWriter(t *testing.T) {
	t.Parallel()

	entry := journal.Entry{Seq: 1, Kind: journal.KindOpen, Payload: json.RawMessage(`{}`)}
	writers := []*JournalSSEWriter{nil, NewJournalSSEWriter(nil, false)}
	for index, writer := range writers {
		if _, err := writer.WriteEntry(entry); err == nil {
			t.Fatalf("writers[%d].WriteEntry() error = nil", index)
		}
		if err := writer.WriteDoneSentinel(); err == nil {
			t.Fatalf("writers[%d].WriteDoneSentinel() error = nil", index)
		}
		if err := writer.WriteReaderLagError(); err == nil {
			t.Fatalf("writers[%d].WriteReaderLagError() error = nil", index)
		}
	}
}

func FuzzJournalSSEWriterChunkRoundTrip(f *testing.F) {
	f.Add("")
	f.Add("one line")
	f.Add("first\n\n雪\n")
	f.Add("nul\x00byte")

	f.Fuzz(func(t *testing.T, data string) {
		if !utf8.ValidString(data) || strings.ContainsRune(data, '\r') {
			return
		}
		payload, err := json.Marshal(map[string]any{
			"data":           data,
			"upstream_event": nil,
			"unknown":        map[string]bool{"retained": true},
		})
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}

		var output bytes.Buffer
		writer := NewJournalSSEWriter(&output, false)
		result, err := writer.WriteEntry(journal.Entry{Seq: 42, Kind: journal.KindChunk, Payload: payload})
		if err != nil {
			t.Fatalf("WriteEntry() error = %v", err)
		}
		if result != (SSEWriteResult{Visible: true}) {
			t.Fatalf("WriteEntry() result = %#v", result)
		}

		events := decodeJournalSSE(t, output.Bytes())
		if len(events) != 1 {
			t.Fatalf("decoded event count = %d, want 1", len(events))
		}
		event := events[0]
		if event.ID != "42" || !event.HasID || event.HasType || string(event.Data) != data {
			t.Fatalf("decoded chunk = %#v, want id 42, no type, data %q", event, data)
		}
	})
}

func decodeJournalSSE(t *testing.T, data []byte) []sse.Event {
	t.Helper()

	decoder := sse.NewDecoder(bytes.NewReader(data))
	var events []sse.Event
	for {
		event, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("Decode() error = %v for wire %q", err, data)
		}
		events = append(events, event)
	}
}

type failOnWrite struct {
	failAt int
	err    error
	calls  int
	output bytes.Buffer
}

func (writer *failOnWrite) Write(data []byte) (int, error) {
	writer.calls++
	if writer.calls == writer.failAt {
		return 0, writer.err
	}
	return writer.output.Write(data)
}
