package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestActiveJournalLeaseRefreshesAndStopsWithRuntime(t *testing.T) {
	t.Parallel()
	rootContext, cancelRoot := context.WithCancel(context.Background())
	t.Cleanup(cancelRoot)
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			JournalTTL:       40 * time.Millisecond,
			ReadinessTimeout: 10 * time.Millisecond,
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	id, err := journal.NewStreamID()
	if err != nil {
		t.Fatalf("journal.NewStreamID() error = %v", err)
	}
	runtime := &streamRuntime{service: service, id: id, done: make(chan struct{})}
	lease := &recordingActiveJournalLease{calls: make(chan struct{}, 8)}
	stopped := make(chan struct{})
	go func() {
		runtime.maintainJournalLease(lease)
		close(stopped)
	}()

	for call := 0; call < 2; call++ {
		select {
		case <-lease.calls:
		case <-time.After(time.Second):
			t.Fatalf("active journal lease call %d did not arrive", call+1)
		}
	}
	close(runtime.done)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("active journal lease did not stop with terminal runtime")
	}
	select {
	case <-lease.calls:
		t.Fatal("active journal lease touched after runtime termination")
	case <-time.After(2 * service.config.JournalTTL):
	}
}

func TestLocalResumeAttachesBeforeBlockingJournalLookup(t *testing.T) {
	t.Parallel()
	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 1 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 10
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	id, err := journal.NewStreamID()
	if err != nil {
		t.Fatalf("journal.NewStreamID() error = %v", err)
	}
	if err := memory.Open(context.Background(), id, journal.Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	blocking := &blockingStateJournal{
		Journal: memory,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	rootContext, cancelRoot := context.WithCancel(context.Background())
	t.Cleanup(cancelRoot)
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			JournalTTL:        time.Minute,
			ReadinessTimeout:  100 * time.Millisecond,
			ReaderMaxLagBytes: 1 << 10,
		},
		journal: blocking,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runtimeContext, cancelRuntime := context.WithCancelCause(rootContext)
	runtime := &streamRuntime{
		service:                    service,
		id:                         id,
		orphanPolicy:               OrphanCancel,
		context:                    runtimeContext,
		cancel:                     cancelRuntime,
		done:                       make(chan struct{}),
		firstEntry:                 make(chan journal.Entry, 1),
		degradedFeed:               newDegradedFeed(service.config.ReaderMaxLagBytes),
		degradationTerminal:        make(chan struct{}),
		degradationTerminalAttempt: make(chan struct{}),
		stopWait:                   make(chan struct{}),
		lastSeq:                    1,
	}
	service.active.Add(1)
	service.streams.Store(id, runtime)
	handler := &Handler{durable: service}
	requestContext, cancelRequest := context.WithCancel(context.Background())
	request := httptest.NewRequestWithContext(
		requestContext, "GET", "/v1/streams/"+id.String()+"/events", nil,
	)
	response := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		handler.serveLocalJournal(response, request, id, 0, false, runtime)
		close(served)
	}()

	select {
	case <-blocking.entered:
	case <-time.After(time.Second):
		t.Fatal("resume request did not reach blocking State")
	}
	runtime.cancelOrphan()
	runtime.mu.Lock()
	readers, terminal := runtime.readers, runtime.terminal
	// Prevent the request cleanup from intentionally exercising OrphanCancel;
	// this test isolates the arrival-vs-State linearization point.
	runtime.orphanPolicy = OrphanContinue
	runtime.mu.Unlock()
	if readers != 1 || terminal != "" || context.Cause(runtimeContext) != nil {
		t.Fatalf("blocked resume state = readers %d, terminal %q, cause %v; orphan cancellation won",
			readers, terminal, context.Cause(runtimeContext))
	}

	cancelRequest()
	close(blocking.release)
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("resume handler did not stop after request cancellation")
	}
}

func TestTerminalWaitsForCommittedAppendResultAndMirrorsExactSequence(t *testing.T) {
	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 1 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 10
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	id, err := journal.NewStreamID()
	if err != nil {
		t.Fatalf("journal.NewStreamID() error = %v", err)
	}
	if err := memory.Open(context.Background(), id, journal.Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	gated := &commitThenBlockAppendJournal{
		Journal:   memory,
		committed: make(chan struct{}),
		returned:  make(chan struct{}),
	}
	t.Cleanup(gated.release)
	rootContext, cancelRoot := context.WithCancel(context.Background())
	t.Cleanup(cancelRoot)
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			JournalTTL:        time.Second,
			ReadinessTimeout:  time.Second,
			ReaderMaxLagBytes: 1 << 10,
		},
		journal: gated,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runtimeContext, cancelRuntime := context.WithCancelCause(rootContext)
	runtime := &streamRuntime{
		service:                    service,
		id:                         id,
		context:                    runtimeContext,
		cancel:                     cancelRuntime,
		done:                       make(chan struct{}),
		firstEntry:                 make(chan journal.Entry, 1),
		degradedFeed:               newDegradedFeed(service.config.ReaderMaxLagBytes),
		degradationTerminal:        make(chan struct{}),
		degradationTerminalAttempt: make(chan struct{}),
		stopWait:                   make(chan struct{}),
		lastSeq:                    1,
	}
	service.active.Add(1)
	subscription, ok := runtime.degradedFeed.subscribe(false)
	if !ok {
		t.Fatal("degradedFeed.subscribe() = false")
	}
	defer subscription.unsubscribe()

	appendResult := make(chan error, 1)
	go func() {
		appendResult <- runtime.appendChunk(sse.Event{
			Data:    []byte(`{"choices":[{"delta":{"content":"x"}}]}`),
			HasData: true,
		}, chunkObservation{})
	}()
	select {
	case <-gated.committed:
	case <-time.After(time.Second):
		t.Fatal("Append did not commit before its gated response")
	}

	terminalResult := make(chan error, 1)
	go func() {
		terminalResult <- runtime.finishError("test_terminal", "test terminal", "test")
	}()
	select {
	case <-runtimeContext.Done():
		t.Fatal("terminal canceled the producer before the committed Append result was observed")
	case <-time.After(50 * time.Millisecond):
	}
	gated.release()
	if appendErr := <-appendResult; appendErr != nil {
		t.Fatalf("appendChunk() error = %v", appendErr)
	}
	if terminalErr := <-terminalResult; terminalErr != nil {
		t.Fatalf("finishError() error = %v", terminalErr)
	}

	entries, err := memory.Read(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	var replay []journal.Entry
	for entry, readErr := range entries {
		if readErr != nil {
			t.Fatalf("Read() iteration error = %v", readErr)
		}
		replay = append(replay, entry)
	}
	if len(replay) != 3 || replay[0].Seq != 1 || replay[0].Kind != journal.KindOpen ||
		replay[1].Seq != 2 || replay[1].Kind != journal.KindChunk ||
		replay[2].Seq != 3 || replay[2].Kind != journal.KindError {
		t.Fatalf("replay = %+v, want open:1 chunk:2 error:3", replay)
	}
	runtime.mu.Lock()
	lastSeq, terminal := runtime.lastSeq, runtime.terminal
	runtime.mu.Unlock()
	if lastSeq != 3 || terminal != journal.KindError {
		t.Fatalf("runtime terminal = (%d, %q), want (3, error)", lastSeq, terminal)
	}

	shadowContext, cancelShadow := context.WithTimeout(context.Background(), time.Second)
	defer cancelShadow()
	chunkFrame, ok, err := subscription.next(shadowContext)
	if err != nil || !ok || chunkFrame.entry == nil ||
		chunkFrame.entry.Seq != 2 || chunkFrame.entry.Kind != journal.KindChunk {
		t.Fatalf("chunk shadow = (%+v, %t, %v), want chunk:2", chunkFrame, ok, err)
	}
	terminalFrame, ok, err := subscription.next(shadowContext)
	if err != nil || !ok || terminalFrame.entry == nil ||
		terminalFrame.entry.Seq != 3 || terminalFrame.entry.Kind != journal.KindError {
		t.Fatalf("terminal shadow = (%+v, %t, %v), want error:3", terminalFrame, ok, err)
	}
}

func TestCommittedAppendResultOutlivesProducerContextCancellation(t *testing.T) {
	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 1 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 10
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	id, err := journal.NewStreamID()
	if err != nil {
		t.Fatalf("journal.NewStreamID() error = %v", err)
	}
	if err := memory.Open(context.Background(), id, journal.Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	gated := &commitThenBlockAppendJournal{
		Journal:   memory,
		committed: make(chan struct{}),
		returned:  make(chan struct{}),
	}
	t.Cleanup(gated.release)
	rootContext, cancelRoot := context.WithCancel(context.Background())
	t.Cleanup(cancelRoot)
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			ReadinessTimeout:  time.Second,
			ReaderMaxLagBytes: 1 << 10,
		},
		journal: gated,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runtimeContext, cancelRuntime := context.WithCancelCause(rootContext)
	runtime := &streamRuntime{
		service:      service,
		id:           id,
		context:      runtimeContext,
		cancel:       cancelRuntime,
		firstEntry:   make(chan journal.Entry, 1),
		degradedFeed: newDegradedFeed(service.config.ReaderMaxLagBytes),
		lastSeq:      1,
	}
	subscription, ok := runtime.degradedFeed.subscribe(false)
	if !ok {
		t.Fatal("degradedFeed.subscribe() = false")
	}
	defer subscription.unsubscribe()

	appendResult := make(chan error, 1)
	go func() {
		appendResult <- runtime.appendChunk(sse.Event{
			Data:    []byte(`{"choices":[{"delta":{"content":"x"}}]}`),
			HasData: true,
		}, chunkObservation{})
	}()
	select {
	case <-gated.committed:
	case <-time.After(time.Second):
		t.Fatal("Append did not commit before its gated response")
	}
	cancelRuntime(errors.New("external producer cancellation"))
	select {
	case appendErr := <-appendResult:
		t.Fatalf("committed Append returned early after producer cancellation: %v", appendErr)
	case <-time.After(50 * time.Millisecond):
	}
	gated.release()
	if appendErr := <-appendResult; appendErr != nil {
		t.Fatalf("appendChunk() error = %v", appendErr)
	}

	runtime.mu.Lock()
	lastSeq := runtime.lastSeq
	runtime.mu.Unlock()
	if lastSeq != 2 {
		t.Fatalf("runtime last sequence = %d, want committed sequence 2", lastSeq)
	}
	shadowContext, cancelShadow := context.WithTimeout(context.Background(), time.Second)
	defer cancelShadow()
	frame, ok, err := subscription.next(shadowContext)
	if err != nil || !ok || frame.entry == nil || frame.entry.Seq != 2 || frame.entry.Kind != journal.KindChunk {
		t.Fatalf("committed shadow = (%+v, %t, %v), want chunk:2", frame, ok, err)
	}
}

type recordingActiveJournalLease struct {
	calls chan struct{}
}

func (lease *recordingActiveJournalLease) Touch(context.Context, journal.StreamID) error {
	lease.calls <- struct{}{}
	return nil
}

type blockingStateJournal struct {
	journal.Journal
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type commitThenBlockAppendJournal struct {
	journal.Journal
	committed chan struct{}
	returned  chan struct{}
	once      sync.Once
}

func (backend *commitThenBlockAppendJournal) Append(
	ctx context.Context,
	id journal.StreamID,
	entry journal.Entry,
) (uint64, error) {
	seq, err := backend.Journal.Append(context.Background(), id, entry)
	if err != nil {
		return 0, err
	}
	backend.once.Do(func() { close(backend.committed) })
	select {
	case <-backend.returned:
		return seq, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (backend *commitThenBlockAppendJournal) release() {
	select {
	case <-backend.returned:
	default:
		close(backend.returned)
	}
}

func (backend *blockingStateJournal) State(
	ctx context.Context,
	id journal.StreamID,
) (journal.StreamState, error) {
	backend.once.Do(func() { close(backend.entered) })
	select {
	case <-backend.release:
		return backend.Journal.State(ctx, id)
	case <-ctx.Done():
		return journal.StreamState{}, ctx.Err()
	}
}
