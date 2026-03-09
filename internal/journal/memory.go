package journal

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"sync"
	"time"
)

// Config controls the bounded memory journal. MaxTotalBytes and
// ReaderMaxLagBytes have no protocol defaults and must be set explicitly.
type Config struct {
	TTL               time.Duration
	MaxBytesPerStream int64
	MaxTotalBytes     int64
	ReaderMaxLagBytes int64
	Clock             func() time.Time
}

// DefaultConfig returns the protocol-defined defaults. Callers must still set
// positive MaxTotalBytes and ReaderMaxLagBytes before constructing a Memory.
func DefaultConfig() Config {
	return Config{
		TTL:               DefaultTTL,
		MaxBytesPerStream: DefaultMaxBytesPerStream,
		Clock:             time.Now,
	}
}

// Memory is a bounded, single-process Journal implementation. It is safe for
// concurrent use.
type Memory struct {
	mu sync.Mutex

	config      Config
	streams     map[StreamID]*memoryStream
	tombstones  map[StreamID]time.Time
	terminalLRU *list.List
	totalBytes  int64
}

type memoryStream struct {
	id             StreamID
	meta           Meta
	originBackend  string
	currentBackend string

	entries     []storedEntry
	bytes       int64
	earliestSeq uint64
	lastSeq     uint64

	status     StreamStatus
	resumable  bool
	createdAt  time.Time
	updatedAt  time.Time
	expiresAt  time.Time
	usage      Usage
	migrations []Migration
	terminal   *TerminalState
	degraded   bool

	tails      map[uint64]*tailReader
	nextTailID uint64
	lruElement *list.Element
}

type storedEntry struct {
	entry Entry
	size  int64
}

type tailReader struct {
	deliveryMu   sync.Mutex
	id           uint64
	stream       *memoryStream
	out          chan Entry
	wake         chan struct{}
	done         chan struct{}
	cursor       uint64
	liveAfter    uint64
	liveLag      int64
	inFlightSeq  uint64
	inFlightSize int64
	stopped      bool
	stopErr      error
	discard      bool
}

type migrationPayload struct {
	FromBackend         string `json:"from_backend"`
	ToBackend           string `json:"to_backend"`
	Reason              string `json:"reason"`
	RescuedTokens       uint64 `json:"rescued_tokens"`
	TokenCountEstimated bool   `json:"token_count_estimated"`
	Attempt             uint64 `json:"attempt"`
}

type usagePayload struct {
	Usage *Usage `json:"usage"`
}

var _ Journal = (*Memory)(nil)
var _ DegradationMarker = (*Memory)(nil)

// NewMemory validates config and returns an empty memory journal.
func NewMemory(config Config) (*Memory, error) {
	config = withDefaults(config)
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return &Memory{
		config:      config,
		streams:     make(map[StreamID]*memoryStream),
		tombstones:  make(map[StreamID]time.Time),
		terminalLRU: list.New(),
	}, nil
}

func withDefaults(config Config) Config {
	if config.TTL == 0 {
		config.TTL = DefaultTTL
	}
	if config.MaxBytesPerStream == 0 {
		config.MaxBytesPerStream = DefaultMaxBytesPerStream
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return config
}

func validateConfig(config Config) error {
	var problems []error
	if config.TTL <= 0 {
		problems = append(problems, errors.New("TTL must be positive"))
	}
	if config.MaxBytesPerStream <= 0 {
		problems = append(problems, errors.New("maximum bytes per stream must be positive"))
	}
	if config.MaxTotalBytes <= 0 {
		problems = append(problems, errors.New("maximum total bytes must be positive"))
	}
	if config.ReaderMaxLagBytes <= 0 {
		problems = append(problems, errors.New("reader maximum lag bytes must be positive"))
	}
	if config.Clock == nil {
		problems = append(problems, errors.New("clock cannot be nil"))
	}
	if len(problems) != 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(problems...))
	}
	return nil
}

// Open creates a stream and commits its open entry as sequence one.
func (m *Memory) Open(ctx context.Context, id StreamID, meta Meta) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	payload, err := json.Marshal(struct {
		StreamID     StreamID `json:"stream_id"`
		Model        string   `json:"model"`
		ModelVersion *string  `json:"model_version"`
		BackendID    string   `json:"backend_id"`
	}{
		StreamID:     id,
		Model:        meta.Model,
		ModelVersion: meta.ModelVersion,
		BackendID:    meta.BackendID,
	})
	if err != nil {
		return fmt.Errorf("marshal open entry: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return err
	}
	now := m.now()
	m.cleanupExpiredLocked(now)
	if _, exists := m.streams[id]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, id)
	}
	if _, exists := m.tombstones[id]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, id)
	}

	entry, size, err := prepareEntry(Entry{Kind: KindOpen, Payload: payload}, 1, now)
	if err != nil {
		return err
	}
	if _, err := m.makeRoomLocked(nil, size, now); err != nil {
		return fmt.Errorf("open stream %s: %w", id, err)
	}

	meta = cloneMeta(meta)
	stream := &memoryStream{
		id:             id,
		meta:           meta,
		originBackend:  meta.BackendID,
		currentBackend: meta.BackendID,
		entries:        []storedEntry{{entry: entry, size: size}},
		bytes:          size,
		earliestSeq:    1,
		lastSeq:        1,
		status:         StatusOpen,
		resumable:      true,
		createdAt:      now,
		updatedAt:      now,
		expiresAt:      now.Add(m.config.TTL),
		migrations:     make([]Migration, 0),
		tails:          make(map[uint64]*tailReader),
	}
	m.streams[id] = stream
	m.totalBytes += size
	return nil
}

// Append commits one non-terminal entry and returns its assigned sequence.
func (m *Memory) Append(ctx context.Context, id StreamID, entry Entry) (uint64, error) {
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if !entry.Kind.appendable() {
		return 0, fmt.Errorf("%w: kind %q cannot be appended", ErrInvalidEntry, entry.Kind)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	now := m.now()
	stream, err := m.lookupLocked(id, now)
	if err != nil {
		return 0, err
	}
	if stream.status != StatusOpen {
		return 0, terminalError(stream)
	}
	if stream.degraded {
		return 0, fmt.Errorf("%w: %s", ErrDegraded, id)
	}
	if stream.lastSeq == math.MaxUint64 {
		return 0, fmt.Errorf("%w: sequence exhausted", ErrCapacity)
	}

	committed, size, err := prepareEntry(entry, stream.lastSeq+1, now)
	if err != nil {
		return 0, err
	}
	if err := m.commitLocked(stream, committed, size, now); err != nil {
		return 0, err
	}
	return committed.Seq, nil
}

// Read returns a finite iterator over a call-time snapshot of entries with
// sequence numbers greater than fromSeq.
func (m *Memory) Read(ctx context.Context, id StreamID, fromSeq uint64) (iter.Seq2[Entry, error], error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	m.mu.Lock()
	if err := checkContext(ctx); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	now := m.now()
	stream, err := m.lookupLocked(id, now)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if !stream.resumable {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrNotResumable, id)
	}
	if err := validateCursor(stream, fromSeq); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.touchTerminalLocked(stream)
	snapshot := entriesAfter(stream, fromSeq)
	degraded := stream.degraded
	if degraded && len(snapshot) == 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: stream %s contains an unjournaled gap", ErrOffsetExpired, id)
	}
	m.mu.Unlock()

	return func(yield func(Entry, error) bool) {
		if err := checkContext(ctx); err != nil {
			yield(Entry{}, err)
			return
		}
		for _, entry := range snapshot {
			if err := checkContext(ctx); err != nil {
				yield(Entry{}, err)
				return
			}
			if !yield(entry, nil) {
				return
			}
		}
		if degraded {
			yield(Entry{}, fmt.Errorf("%w: stream %s contains an unjournaled gap", ErrOffsetExpired, id))
		}
	}, nil
}

// Tail atomically attaches a reader to retained replay and future commits.
func (m *Memory) Tail(ctx context.Context, id StreamID, fromSeq uint64) (<-chan Entry, func(), error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, err
	}

	m.mu.Lock()
	if err := checkContext(ctx); err != nil {
		m.mu.Unlock()
		return nil, nil, err
	}
	now := m.now()
	stream, err := m.lookupLocked(id, now)
	if err != nil {
		m.mu.Unlock()
		return nil, nil, err
	}
	if !stream.resumable {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: %s", ErrNotResumable, id)
	}
	if err := validateCursor(stream, fromSeq); err != nil {
		m.mu.Unlock()
		return nil, nil, err
	}
	if stream.degraded && fromSeq == stream.lastSeq {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: stream %s contains an unjournaled gap", ErrOffsetExpired, id)
	}
	if stream.nextTailID == math.MaxUint64 {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: reader identifier exhausted", ErrCapacity)
	}
	stream.nextTailID++
	reader := &tailReader{
		id:        stream.nextTailID,
		stream:    stream,
		out:       make(chan Entry, 1),
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		cursor:    fromSeq,
		liveAfter: stream.lastSeq,
	}
	stream.tails[reader.id] = reader
	m.touchTerminalLocked(stream)
	m.mu.Unlock()

	go m.runTail(ctx, reader)
	cancel := func() {
		m.mu.Lock()
		m.stopTailLocked(reader, nil, true)
		m.mu.Unlock()
	}
	return reader.out, cancel, nil
}

// State returns an immutable point-in-time state snapshot.
func (m *Memory) State(ctx context.Context, id StreamID) (StreamState, error) {
	if err := checkContext(ctx); err != nil {
		return StreamState{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return StreamState{}, err
	}
	now := m.now()
	stream, err := m.lookupLocked(id, now)
	if err != nil {
		return StreamState{}, err
	}
	m.touchTerminalLocked(stream)
	return snapshotState(stream), nil
}

// Close commits exactly one terminal entry and marks the stream closed.
func (m *Memory) Close(ctx context.Context, id StreamID, terminal Entry) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if !terminal.Kind.terminal() {
		return fmt.Errorf("%w: kind %q is not terminal", ErrInvalidEntry, terminal.Kind)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return err
	}
	now := m.now()
	stream, err := m.lookupLocked(id, now)
	if err != nil {
		return err
	}
	if stream.status != StatusOpen {
		return terminalError(stream)
	}
	if stream.degraded {
		return fmt.Errorf("%w: %s", ErrDegraded, id)
	}
	if stream.lastSeq == math.MaxUint64 {
		return fmt.Errorf("%w: sequence exhausted", ErrCapacity)
	}

	committed, size, err := prepareEntry(terminal, stream.lastSeq+1, now)
	if err != nil {
		return err
	}
	if err := m.commitLocked(stream, committed, size, now); err != nil {
		return err
	}
	stream.status = statusForTerminal(committed.Kind)
	stream.resumable = committed.Kind != KindStopped
	stream.expiresAt = now.Add(m.config.TTL)
	stream.terminal = &TerminalState{
		Seq:     committed.Seq,
		TS:      committed.TS,
		Kind:    committed.Kind,
		Payload: bytes.Clone(committed.Payload),
	}
	stream.lruElement = m.terminalLRU.PushFront(stream)
	return nil
}

// MarkDegraded permanently seals sequence allocation after a post-open
// journal gap while retaining the committed prefix for diagnostic replay.
func (m *Memory) MarkDegraded(ctx context.Context, id StreamID) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return err
	}
	now := m.now()
	stream, err := m.lookupLocked(id, now)
	if err != nil {
		return err
	}
	if stream.status != StatusOpen {
		return terminalError(stream)
	}
	if stream.degraded {
		stream.updatedAt = now
		stream.expiresAt = now.Add(m.config.TTL)
		return nil
	}
	stream.degraded = true
	stream.updatedAt = now
	stream.expiresAt = now.Add(m.config.TTL)
	for _, reader := range stream.tails {
		select {
		case reader.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *Memory) commitLocked(stream *memoryStream, entry Entry, size int64, now time.Time) error {
	trimCount, err := m.makeRoomLocked(stream, size, now)
	if err != nil {
		return fmt.Errorf("append to stream %s: %w", stream.id, err)
	}
	if trimCount != 0 {
		m.trimStreamLocked(stream, trimCount)
	}
	stream.entries = append(stream.entries, storedEntry{entry: entry, size: size})
	stream.bytes += size
	m.totalBytes += size
	stream.lastSeq = entry.Seq
	if len(stream.entries) == 1 {
		stream.earliestSeq = entry.Seq
	}
	stream.updatedAt = now
	stream.expiresAt = now.Add(m.config.TTL)
	m.applyStateEntryLocked(stream, entry)
	m.dropReadersBehindRingLocked(stream)
	m.publishLocked(stream, size)
	return nil
}

func (m *Memory) makeRoomLocked(stream *memoryStream, size int64, now time.Time) (int, error) {
	if size > m.config.MaxBytesPerStream {
		return 0, fmt.Errorf("%w: entry needs %d bytes; per-stream limit is %d", ErrCapacity, size, m.config.MaxBytesPerStream)
	}

	trimCount := 0
	trimBytes := int64(0)
	if stream != nil {
		remainingLimit := m.config.MaxBytesPerStream - size
		for stream.bytes-trimBytes > remainingLimit {
			trimBytes += stream.entries[trimCount].size
			trimCount++
		}
	}

	baseBytes := m.totalBytes - trimBytes
	needFree := int64(0)
	if size > m.config.MaxTotalBytes-baseBytes {
		needFree = size - (m.config.MaxTotalBytes - baseBytes)
	}

	var victims []*memoryStream
	freed := int64(0)
	for element := m.terminalLRU.Back(); element != nil && freed < needFree; element = element.Prev() {
		candidate := element.Value.(*memoryStream)
		if candidate == stream {
			continue
		}
		victims = append(victims, candidate)
		freed += candidate.bytes
	}
	if freed < needFree {
		return 0, fmt.Errorf("%w: need %d additional bytes with only active streams remaining", ErrCapacity, needFree-freed)
	}
	for _, victim := range victims {
		m.removeStreamLocked(victim, now, now.Add(m.config.TTL))
	}
	return trimCount, nil
}

func (m *Memory) trimStreamLocked(stream *memoryStream, count int) {
	for _, stored := range stream.entries[:count] {
		stream.bytes -= stored.size
		m.totalBytes -= stored.size
	}
	copy(stream.entries, stream.entries[count:])
	stream.entries = stream.entries[:len(stream.entries)-count]
	if len(stream.entries) != 0 {
		stream.earliestSeq = stream.entries[0].entry.Seq
	} else {
		stream.earliestSeq = stream.lastSeq + 1
	}
}

func (m *Memory) publishLocked(stream *memoryStream, size int64) {
	for _, reader := range stream.tails {
		reader.liveLag += size
		// The entry currently handed to the reader goroutine is no longer in
		// the journal-to-reader backlog, even if that goroutine has not yet
		// reacquired m.mu to finish its bookkeeping.
		effectiveLag := reader.liveLag - reader.inFlightSize
		if effectiveLag > m.config.ReaderMaxLagBytes {
			m.stopTailLocked(reader, ErrReaderLagged, true)
			continue
		}
		select {
		case reader.wake <- struct{}{}:
		default:
		}
	}
}

func (m *Memory) dropReadersBehindRingLocked(stream *memoryStream) {
	if stream.earliestSeq <= 1 {
		return
	}
	minimumCursor := stream.earliestSeq - 1
	for _, reader := range stream.tails {
		effectiveCursor := reader.cursor
		if reader.inFlightSeq > effectiveCursor {
			effectiveCursor = reader.inFlightSeq
		}
		if effectiveCursor < minimumCursor {
			m.stopTailLocked(reader, ErrReaderLagged, true)
		}
	}
}

func (m *Memory) runTail(ctx context.Context, reader *tailReader) {
	defer m.finishTail(reader)
	for {
		m.mu.Lock()
		if reader.stopped {
			m.mu.Unlock()
			return
		}
		stream := reader.stream
		if stream.earliestSeq > 1 && reader.cursor < stream.earliestSeq-1 {
			m.stopTailLocked(reader, ErrReaderLagged, true)
			m.mu.Unlock()
			return
		}
		if reader.cursor < stream.lastSeq {
			nextSeq := reader.cursor + 1
			index := nextSeq - stream.earliestSeq
			if index >= uint64(len(stream.entries)) {
				m.stopTailLocked(reader, ErrReaderLagged, true)
				m.mu.Unlock()
				return
			}
			stored := stream.entries[index]
			entry := cloneEntry(stored.entry)
			reader.inFlightSeq = entry.Seq
			reader.inFlightSize = stored.size
			m.mu.Unlock()

			reader.deliveryMu.Lock()
			canceled := false
			select {
			case reader.out <- entry:
			case <-reader.done:
				reader.deliveryMu.Unlock()
				return
			case <-ctx.Done():
				canceled = true
			}
			reader.deliveryMu.Unlock()
			if canceled {
				m.mu.Lock()
				m.stopTailLocked(reader, nil, true)
				m.mu.Unlock()
				return
			}

			m.mu.Lock()
			if !reader.stopped {
				reader.cursor = entry.Seq
				if entry.Seq > reader.liveAfter {
					reader.liveLag -= reader.inFlightSize
					if reader.liveLag < 0 {
						reader.liveLag = 0
					}
				}
				reader.inFlightSeq = 0
				reader.inFlightSize = 0
				if entry.Kind.terminal() {
					m.stopTailLocked(reader, nil, false)
				}
			}
			stopped := reader.stopped
			m.mu.Unlock()
			if stopped {
				return
			}
			continue
		}
		if stream.status != StatusOpen {
			m.stopTailLocked(reader, nil, false)
			m.mu.Unlock()
			return
		}
		if stream.degraded {
			m.stopTailLocked(reader, ErrOffsetExpired, false)
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()

		select {
		case <-reader.wake:
		case <-reader.done:
			return
		case <-ctx.Done():
			m.mu.Lock()
			m.stopTailLocked(reader, nil, true)
			m.mu.Unlock()
			return
		}
	}
}

func (m *Memory) stopTailLocked(reader *tailReader, reason error, discard bool) {
	if reader.stopped {
		return
	}
	reader.stopped = true
	reader.stopErr = reason
	reader.discard = discard
	delete(reader.stream.tails, reader.id)
	close(reader.done)
	if discard {
		reader.deliveryMu.Lock()
		select {
		case <-reader.out:
		default:
		}
		reader.deliveryMu.Unlock()
		reader.discard = false
	}
}

func (m *Memory) finishTail(reader *tailReader) {
	m.mu.Lock()
	if !reader.stopped {
		m.stopTailLocked(reader, nil, true)
	}
	reason := reader.stopErr
	discard := reader.discard
	m.mu.Unlock()

	if discard {
		select {
		case <-reader.out:
		default:
		}
	}
	if reason != nil {
		reader.out <- Entry{Err: reason}
	}
	close(reader.out)
}

func (m *Memory) applyStateEntryLocked(stream *memoryStream, entry Entry) {
	if entry.Kind == KindMigration {
		var payload migrationPayload
		if json.Unmarshal(entry.Payload, &payload) == nil {
			stream.migrations = append(stream.migrations, Migration{
				Seq:                 entry.Seq,
				FromBackend:         payload.FromBackend,
				ToBackend:           payload.ToBackend,
				Reason:              payload.Reason,
				RescuedTokens:       payload.RescuedTokens,
				TokenCountEstimated: payload.TokenCountEstimated,
				Attempt:             payload.Attempt,
			})
			stream.currentBackend = payload.ToBackend
		}
	}
	var payload usagePayload
	if json.Unmarshal(entry.Payload, &payload) == nil && payload.Usage != nil {
		stream.usage = *payload.Usage
	}
}

func (m *Memory) lookupLocked(id StreamID, now time.Time) (*memoryStream, error) {
	m.cleanupExpiredLocked(now)
	if stream, ok := m.streams[id]; ok {
		return stream, nil
	}
	if _, ok := m.tombstones[id]; ok {
		return nil, fmt.Errorf("%w: %s", ErrExpired, id)
	}
	return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
}

func (m *Memory) cleanupExpiredLocked(now time.Time) {
	for id, expiresAt := range m.tombstones {
		if !now.Before(expiresAt) {
			delete(m.tombstones, id)
		}
	}
	for _, stream := range m.streams {
		if (stream.status == StatusOpen && !stream.degraded) || now.Before(stream.expiresAt) {
			continue
		}
		tombstoneUntil := stream.expiresAt.Add(m.config.TTL)
		m.removeStreamLocked(stream, now, tombstoneUntil)
	}
}

func (m *Memory) removeStreamLocked(stream *memoryStream, now, tombstoneUntil time.Time) {
	for _, reader := range stream.tails {
		m.stopTailLocked(reader, ErrExpired, true)
	}
	delete(m.streams, stream.id)
	m.totalBytes -= stream.bytes
	stream.entries = nil
	stream.bytes = 0
	if stream.lruElement != nil {
		m.terminalLRU.Remove(stream.lruElement)
		stream.lruElement = nil
	}
	if now.Before(tombstoneUntil) {
		if existing, ok := m.tombstones[stream.id]; !ok || existing.Before(tombstoneUntil) {
			m.tombstones[stream.id] = tombstoneUntil
		}
	}
}

func (m *Memory) touchTerminalLocked(stream *memoryStream) {
	if stream.lruElement != nil {
		m.terminalLRU.MoveToFront(stream.lruElement)
	}
}

func (m *Memory) now() time.Time {
	return m.config.Clock().UTC()
}

func prepareEntry(input Entry, seq uint64, now time.Time) (Entry, int64, error) {
	if input.Err != nil {
		return Entry{}, 0, fmt.Errorf("%w: persisted entries cannot carry delivery errors", ErrInvalidEntry)
	}
	if err := validatePayload(input.Payload); err != nil {
		return Entry{}, 0, err
	}
	entry := Entry{
		Seq:     seq,
		TS:      now,
		Kind:    input.Kind,
		Payload: bytes.Clone(input.Payload),
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("%w: marshal envelope: %w", ErrInvalidEntry, err)
	}
	return entry, int64(len(encoded)), nil
}

func validatePayload(payload json.RawMessage) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !json.Valid(trimmed) {
		return fmt.Errorf("%w: payload must be a valid JSON object", ErrInvalidEntry)
	}
	return nil
}

func validateCursor(stream *memoryStream, fromSeq uint64) error {
	if fromSeq > stream.lastSeq {
		return fmt.Errorf("%w: cursor %d exceeds last sequence %d", ErrCursorAhead, fromSeq, stream.lastSeq)
	}
	if stream.earliestSeq > 1 && fromSeq < stream.earliestSeq-1 {
		return fmt.Errorf("%w: cursor %d precedes earliest sequence %d", ErrOffsetExpired, fromSeq, stream.earliestSeq)
	}
	return nil
}

func entriesAfter(stream *memoryStream, fromSeq uint64) []Entry {
	entries := make([]Entry, 0, len(stream.entries))
	for _, stored := range stream.entries {
		if stored.entry.Seq > fromSeq {
			entries = append(entries, cloneEntry(stored.entry))
		}
	}
	return entries
}

func snapshotState(stream *memoryStream) StreamState {
	state := StreamState{
		StreamID:       stream.id,
		Status:         stream.status,
		Resumable:      stream.resumable,
		Model:          stream.meta.Model,
		ModelVersion:   cloneString(stream.meta.ModelVersion),
		OriginBackend:  stream.originBackend,
		CurrentBackend: stream.currentBackend,
		CreatedAt:      stream.createdAt,
		UpdatedAt:      stream.updatedAt,
		EarliestSeq:    stream.earliestSeq,
		LastSeq:        stream.lastSeq,
		Usage:          stream.usage,
		Migrations:     append([]Migration(nil), stream.migrations...),
	}
	if stream.terminal != nil {
		terminal := *stream.terminal
		terminal.Payload = bytes.Clone(terminal.Payload)
		state.Terminal = &terminal
	}
	return state
}

func cloneMeta(meta Meta) Meta {
	meta.ModelVersion = cloneString(meta.ModelVersion)
	if meta.Idempotency != nil {
		digest := *meta.Idempotency
		meta.Idempotency = &digest
	}
	meta.Request = bytes.Clone(meta.Request)
	return meta
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneEntry(entry Entry) Entry {
	entry.Payload = bytes.Clone(entry.Payload)
	return entry
}

func statusForTerminal(kind EntryKind) StreamStatus {
	switch kind {
	case KindDone:
		return StatusDone
	case KindError:
		return StatusError
	case KindStopped:
		return StatusStopped
	default:
		return StatusOpen
	}
}

func terminalError(stream *memoryStream) error {
	return fmt.Errorf("%w: stream %s ended as %s at sequence %d", ErrTerminalState, stream.id, stream.status, stream.lastSeq)
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidContext
	}
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.Canceled
	default:
		return nil
	}
}
