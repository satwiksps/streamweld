package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/satwiksps/streamweld/internal/journal"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

const (
	streamOpenEvent      = "streamweld.stream.open"
	streamMigrationEvent = "streamweld.stream.migration"
	streamWarningEvent   = "streamweld.stream.warning"
	streamErrorEvent     = "streamweld.stream.error"
	streamDoneEvent      = "streamweld.stream.done"
	streamStoppedEvent   = "streamweld.stream.stopped"
	readerErrorEvent     = "streamweld.reader.error"

	doneSentinelData   = "[DONE]"
	readerLagErrorData = `{"code":"reader_lag_exceeded"}`
)

var (
	// ErrInvalidJournalEntry indicates that an entry cannot be represented by
	// the downstream journal-to-SSE protocol mapping.
	ErrInvalidJournalEntry = errors.New("proxy: invalid journal entry")

	// ErrMalformedJournalPayload indicates that a journal payload is not the
	// JSON object required by the journal contract, or that a chunk does not
	// contain a string data field.
	ErrMalformedJournalPayload = errors.New("proxy: malformed journal payload")
)

// SSEWriteResult describes a successfully mapped journal entry. Invisible
// entries are valid and produce a zero result. Terminal is true for error,
// done, and stopped entries.
type SSEWriteResult struct {
	Visible  bool
	Terminal bool
}

// JournalSSEWriter maps committed journal entries to complete downstream SSE
// frames. It does not flush or close the underlying writer; those remain the
// responsibility of the HTTP streaming layer.
type JournalSSEWriter struct {
	encoder *sse.Encoder
	verbose bool
}

// NewJournalSSEWriter returns a journal-to-SSE writer. Migration and warning
// entries are emitted only when verbose is true.
func NewJournalSSEWriter(writer io.Writer, verbose bool) *JournalSSEWriter {
	var encoder *sse.Encoder
	if writer != nil {
		encoder = sse.NewEncoder(writer)
	}
	return &JournalSSEWriter{encoder: encoder, verbose: verbose}
}

// WriteEntry validates entry and writes its protocol representation. A done
// entry writes both its sequenced control event and the unsequenced [DONE]
// compatibility sentinel. The returned result is meaningful only when err is
// nil.
func (writer *JournalSSEWriter) WriteEntry(entry journal.Entry) (SSEWriteResult, error) {
	if err := writer.ready(); err != nil {
		return SSEWriteResult{}, err
	}
	if entry.Seq == 0 {
		return SSEWriteResult{}, fmt.Errorf("%w: sequence must be positive", ErrInvalidJournalEntry)
	}
	if entry.Err != nil {
		return SSEWriteResult{}, fmt.Errorf("%w: sequence %d carries delivery error: %w", ErrInvalidJournalEntry, entry.Seq, entry.Err)
	}

	mapping, ok := journalSSEMappings[entry.Kind]
	if !ok {
		return SSEWriteResult{}, fmt.Errorf("%w: sequence %d has unsupported kind %q", ErrInvalidJournalEntry, entry.Seq, entry.Kind)
	}

	payload, err := downstreamPayload(entry)
	if err != nil {
		return SSEWriteResult{}, err
	}
	if mapping.verboseOnly && !writer.verbose {
		return SSEWriteResult{}, nil
	}

	event := sse.Event{
		Data:    payload,
		ID:      strconv.FormatUint(entry.Seq, 10),
		HasData: true,
		HasID:   true,
	}
	if mapping.eventType != "" {
		event.Type = mapping.eventType
		event.HasType = true
	}
	if err := writer.encoder.Encode(event); err != nil {
		return SSEWriteResult{}, fmt.Errorf("proxy: write journal sequence %d as SSE: %w", entry.Seq, err)
	}
	if entry.Kind == journal.KindDone {
		if err := writer.WriteDoneSentinel(); err != nil {
			return SSEWriteResult{}, err
		}
	}

	return SSEWriteResult{Visible: true, Terminal: mapping.terminal}, nil
}

// WriteDoneSentinel writes the unsequenced OpenAI compatibility marker. It is
// also used when a successful resume cursor already equals the terminal
// sequence and therefore has no journal entry left to replay.
func (writer *JournalSSEWriter) WriteDoneSentinel() error {
	if err := writer.ready(); err != nil {
		return err
	}
	if err := writer.encoder.Encode(sse.Event{Data: []byte(doneSentinelData), HasData: true}); err != nil {
		return fmt.Errorf("proxy: write done sentinel as SSE: %w", err)
	}
	return nil
}

// WriteReaderLagError writes the best-effort unsequenced control event used
// when one downstream reader exceeds its independent lag limit.
func (writer *JournalSSEWriter) WriteReaderLagError() error {
	if err := writer.ready(); err != nil {
		return err
	}
	event := sse.Event{
		Data:    []byte(readerLagErrorData),
		Type:    readerErrorEvent,
		HasData: true,
		HasType: true,
	}
	if err := writer.encoder.Encode(event); err != nil {
		return fmt.Errorf("proxy: write reader lag error as SSE: %w", err)
	}
	return nil
}

// WriteDegradedFrame writes a process-local frame after sequence allocation
// has permanently stopped. Such frames deliberately never carry an SSE id.
func (writer *JournalSSEWriter) WriteDegradedFrame(frame degradedFrame) (SSEWriteResult, error) {
	if err := writer.ready(); err != nil {
		return SSEWriteResult{}, err
	}
	if frame.verboseOnly && !writer.verbose {
		return SSEWriteResult{}, nil
	}
	event := frame.event
	event.ID = ""
	event.HasID = false
	if err := writer.encoder.Encode(event); err != nil {
		return SSEWriteResult{}, fmt.Errorf("proxy: write degraded SSE frame: %w", err)
	}
	return SSEWriteResult{Visible: true, Terminal: frame.terminal}, nil
}

// WriteOffsetExpiredError terminates an already-started replay after its
// committed prefix. HTTP status cannot change after that prefix is written, so
// the 410 condition is represented as an unsequenced stream error. A cursor at
// the end of the prefix is rejected with HTTP 410 before streaming begins.
func (writer *JournalSSEWriter) WriteOffsetExpiredError(streamID journal.StreamID) error {
	payload, err := json.Marshal(struct {
		Code     string `json:"code"`
		Message  string `json:"message"`
		StreamID string `json:"stream_id"`
	}{
		Code:     "stream_offset_expired",
		Message:  "stream contains an unjournaled gap",
		StreamID: streamID.String(),
	})
	if err != nil {
		return err
	}
	_, err = writer.WriteDegradedFrame(degradedFrame{
		event: sse.Event{
			Type:    streamErrorEvent,
			Data:    payload,
			HasType: true,
			HasData: true,
		},
		terminal: true,
	})
	return err
}

func (writer *JournalSSEWriter) ready() error {
	if writer == nil || writer.encoder == nil {
		return errors.New("proxy: journal SSE writer is not initialized")
	}
	return nil
}

type journalSSEMapping struct {
	eventType   string
	verboseOnly bool
	terminal    bool
}

var journalSSEMappings = map[journal.EntryKind]journalSSEMapping{
	journal.KindOpen:      {eventType: streamOpenEvent},
	journal.KindChunk:     {},
	journal.KindMigration: {eventType: streamMigrationEvent, verboseOnly: true},
	journal.KindWarning:   {eventType: streamWarningEvent, verboseOnly: true},
	journal.KindError:     {eventType: streamErrorEvent, terminal: true},
	journal.KindDone:      {eventType: streamDoneEvent, terminal: true},
	journal.KindStopped:   {eventType: streamStoppedEvent, terminal: true},
}

func downstreamPayload(entry journal.Entry) ([]byte, error) {
	trimmed := bytes.TrimSpace(entry.Payload)
	if !utf8.Valid(entry.Payload) || len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !json.Valid(trimmed) {
		return nil, fmt.Errorf("%w: sequence %d kind %q must contain a valid UTF-8 JSON object", ErrMalformedJournalPayload, entry.Seq, entry.Kind)
	}
	if entry.Kind != journal.KindChunk {
		return entry.Payload, nil
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return nil, fmt.Errorf("%w: sequence %d chunk object: %w", ErrMalformedJournalPayload, entry.Seq, err)
	}
	rawData, ok := fields["data"]
	if !ok {
		return nil, fmt.Errorf("%w: sequence %d chunk is missing string field %q", ErrMalformedJournalPayload, entry.Seq, "data")
	}
	var data *string
	if err := json.Unmarshal(rawData, &data); err != nil || data == nil {
		if err != nil {
			return nil, fmt.Errorf("%w: sequence %d chunk field %q: %w", ErrMalformedJournalPayload, entry.Seq, "data", err)
		}
		return nil, fmt.Errorf("%w: sequence %d chunk field %q must be a string", ErrMalformedJournalPayload, entry.Seq, "data")
	}
	return []byte(*data), nil
}
