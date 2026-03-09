package journal

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"time"
)

const (
	// DefaultTTL is the protocol retention period for terminal journals.
	DefaultTTL = 10 * time.Minute

	// DefaultMaxBytesPerStream is the protocol memory-ring limit per stream.
	DefaultMaxBytesPerStream int64 = 4 << 20
)

var (
	// ErrAlreadyExists means Open observed the stream ID before.
	ErrAlreadyExists = errors.New("journal: stream already exists")
	// ErrNotFound means neither a journal nor an expiry tombstone exists.
	ErrNotFound = errors.New("journal: stream not found")
	// ErrExpired means event data expired but its existence tombstone remains.
	ErrExpired = errors.New("journal: stream expired")
	// ErrOffsetExpired means the requested cursor predates retained ring data.
	ErrOffsetExpired = errors.New("journal: stream offset expired")
	// ErrCursorAhead means the requested cursor exceeds the committed sequence.
	ErrCursorAhead = errors.New("journal: cursor is ahead of stream")
	// ErrNotResumable means the stream was explicitly stopped.
	ErrNotResumable = errors.New("journal: stream is not resumable")
	// ErrTerminalState means a terminal entry has already won for the stream.
	ErrTerminalState = errors.New("journal: stream is already terminal")
	// ErrInvalidEntry means an entry kind or payload violates the journal contract.
	ErrInvalidEntry = errors.New("journal: invalid entry")
	// ErrCapacity means a hard memory or sequence capacity was exhausted.
	ErrCapacity = errors.New("journal: capacity exceeded")
	// ErrReaderLagged means a tail reader exceeded its independent lag budget.
	ErrReaderLagged = errors.New("journal: reader lag exceeded")
	// ErrDegraded means a post-open durability gap permanently prevents more
	// sequence allocation for the stream.
	ErrDegraded = errors.New("journal: stream durability degraded")
	// ErrInvalidConfig means a memory-backend setting is invalid or missing.
	ErrInvalidConfig = errors.New("journal: invalid configuration")
	// ErrInvalidContext means a nil context was supplied.
	ErrInvalidContext = errors.New("journal: nil context")
)

// EntryKind identifies a journal entry's protocol meaning.
type EntryKind string

const (
	// KindOpen records immutable metadata and is created only by Open.
	KindOpen EntryKind = "open"
	// KindChunk records one complete upstream SSE event.
	KindChunk EntryKind = "chunk"
	// KindMigration records a producer handoff before continuation chunks.
	KindMigration EntryKind = "migration"
	// KindWarning records a nonterminal protocol warning.
	KindWarning EntryKind = "warning"
	// KindError records terminal generation failure.
	KindError EntryKind = "error"
	// KindDone records successful terminal completion.
	KindDone EntryKind = "done"
	// KindStopped records an explicit, non-resumable stop.
	KindStopped EntryKind = "stopped"
)

// Entry is the persisted journal envelope. Seq and TS are assigned by the
// journal. Payload must be a valid JSON object and is retained byte-for-byte.
//
// Err is never persisted or marshaled. Tail uses it for an unsequenced delivery
// failure, most notably ErrReaderLagged, immediately before closing that reader.
type Entry struct {
	Seq     uint64          `json:"seq"`
	TS      time.Time       `json:"ts"`
	Kind    EntryKind       `json:"kind"`
	Payload json.RawMessage `json:"payload"`
	Err     error           `json:"-"`
}

// Meta is immutable stream metadata captured by Open.
type Meta struct {
	Model        string       `json:"model"`
	ModelVersion *string      `json:"model_version"`
	BackendID    string       `json:"backend_id"`
	Owner        *OwnerRecord `json:"-"`
	// Idempotency identifies a pending reservation that Open must atomically
	// promote with the journal. It is private coordination metadata and is never
	// returned in the open entry or public stream state.
	Idempotency *IdempotencyDigest `json:"-"`

	// Endpoint and Request are private producer-recovery metadata. Journal
	// implementations may persist them, but they are never included in the open
	// entry or StreamState returned to clients.
	Endpoint string          `json:"-"`
	Request  json.RawMessage `json:"-"`
}

// Usage is the latest token accounting exposed by StreamState.
type Usage struct {
	PromptTokens     uint64 `json:"prompt_tokens"`
	CompletionTokens uint64 `json:"completion_tokens"`
	TotalTokens      uint64 `json:"total_tokens"`
	Estimated        bool   `json:"estimated"`
}

// Migration is a state snapshot of a committed migration entry.
type Migration struct {
	Seq                 uint64 `json:"seq"`
	FromBackend         string `json:"from_backend"`
	ToBackend           string `json:"to_backend"`
	Reason              string `json:"reason"`
	RescuedTokens       uint64 `json:"rescued_tokens"`
	TokenCountEstimated bool   `json:"token_count_estimated"`
	Attempt             uint64 `json:"attempt"`
}

// StreamStatus is the lifecycle state exposed by State.
type StreamStatus string

const (
	// StatusOpen means the producer may still append entries.
	StatusOpen StreamStatus = "open"
	// StatusDone means successful completion is terminal.
	StatusDone StreamStatus = "done"
	// StatusError means generation failure is terminal.
	StatusError StreamStatus = "error"
	// StatusStopped means an explicit stop is terminal and non-resumable.
	StatusStopped StreamStatus = "stopped"
)

// TerminalState identifies the winning terminal entry.
type TerminalState struct {
	Seq     uint64          `json:"seq"`
	TS      time.Time       `json:"ts"`
	Kind    EntryKind       `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// StreamState is an immutable point-in-time stream snapshot.
type StreamState struct {
	StreamID       StreamID       `json:"stream_id"`
	Status         StreamStatus   `json:"status"`
	Resumable      bool           `json:"resumable"`
	Model          string         `json:"model"`
	ModelVersion   *string        `json:"model_version"`
	OriginBackend  string         `json:"origin_backend"`
	CurrentBackend string         `json:"current_backend"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	EarliestSeq    uint64         `json:"earliest_seq"`
	LastSeq        uint64         `json:"last_seq"`
	Usage          Usage          `json:"usage"`
	Migrations     []Migration    `json:"migrations"`
	Terminal       *TerminalState `json:"terminal"`
}

// Journal is the durable stream log contract shared by memory and Redis
// backends. Read is finite at its call-time snapshot; Tail atomically bridges
// retained replay into live fan-out.
type Journal interface {
	Open(ctx context.Context, id StreamID, meta Meta) error
	Append(ctx context.Context, id StreamID, entry Entry) (seq uint64, err error)
	Read(ctx context.Context, id StreamID, fromSeq uint64) (iter.Seq2[Entry, error], error)
	Tail(ctx context.Context, id StreamID, fromSeq uint64) (<-chan Entry, func(), error)
	State(ctx context.Context, id StreamID) (StreamState, error)
	Close(ctx context.Context, id StreamID, terminal Entry) error
}

// DegradationMarker is an optional extension implemented by journals that can
// remember a post-open durability gap. Once marked, a stream must never assign
// another sequence. Read and Tail may replay its committed prefix, but must end
// with ErrOffsetExpired so callers cannot mistake the prefix for a complete
// generation.
type DegradationMarker interface {
	MarkDegraded(ctx context.Context, id StreamID) error
}

// ActiveJournalLease is an optional extension for durable stores whose active
// keys expire server-side. Touch atomically renews all keys required to keep an
// open journal replayable. Implementations must reject terminal journals so a
// late refresher cannot extend terminal retention.
type ActiveJournalLease interface {
	Touch(ctx context.Context, id StreamID) error
}

func (kind EntryKind) terminal() bool {
	return kind == KindDone || kind == KindError || kind == KindStopped
}

func (kind EntryKind) appendable() bool {
	return kind == KindChunk || kind == KindMigration || kind == KindWarning
}
