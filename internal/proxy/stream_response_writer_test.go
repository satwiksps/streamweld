package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
)

func TestStreamResponseWriterBoundsWritesAndFlushes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		blockWrite bool
		blockFlush bool
		operation  func(*streamResponseWriter) error
	}{
		{
			name:       "write",
			blockWrite: true,
			operation: func(writer *streamResponseWriter) error {
				_, err := writer.Write([]byte("event"))
				return err
			},
		},
		{
			name:       "flush",
			blockFlush: true,
			operation:  (*streamResponseWriter).Flush,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := newDeadlineBlockingResponseWriter(test.blockWrite, test.blockFlush)
			writer := newStreamResponseWriter(response, 20*time.Millisecond)
			started := time.Now()
			err := test.operation(writer)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("operation error = %v, want deadline exceeded", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("deadline-bound operation took %s", elapsed)
			}
		})
	}
}

func TestBlockedDownstreamWriterDetachesWithoutBlockingProducerAppend(t *testing.T) {
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

	rootContext, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			JournalTTL:         time.Minute,
			ReadinessTimeout:   time.Second,
			ReaderMaxLagBytes:  1 << 10,
			ReaderWriteTimeout: 100 * time.Millisecond,
		},
		journal: memory,
		logger:  slog.New(slog.DiscardHandler),
	}
	runtimeContext, cancelRuntime := context.WithCancelCause(rootContext)
	defer cancelRuntime(context.Canceled)
	runtime := &streamRuntime{
		service:      service,
		id:           id,
		orphanPolicy: OrphanContinue,
		context:      runtimeContext,
		cancel:       cancelRuntime,
		degradedFeed: newDegradedFeed(service.config.ReaderMaxLagBytes),
		lastSeq:      1,
	}
	handler := &Handler{durable: service}
	response := newDeadlineBlockingResponseWriter(true, false)
	request := httptest.NewRequestWithContext(
		context.Background(), http.MethodGet, "/v1/streams/"+id.String()+"/events", nil,
	)
	served := make(chan struct{})
	go func() {
		handler.serveLocalJournal(response, request, id, 0, false, runtime)
		close(served)
	}()

	select {
	case <-response.writeEntered:
	case <-time.After(time.Second):
		t.Fatal("resume handler did not reach the blocking downstream write")
	}
	appendResult := make(chan error, 1)
	go func() {
		_, appendErr := memory.Append(context.Background(), id, journal.Entry{
			Kind: journal.KindChunk, Payload: []byte(`{"data":"x"}`),
		})
		appendResult <- appendErr
	}()
	select {
	case appendErr := <-appendResult:
		if appendErr != nil {
			t.Fatalf("producer Append() error = %v", appendErr)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("producer Append blocked behind the stalled downstream writer")
	}
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("resume handler did not stop at its downstream write deadline")
	}
	runtime.mu.Lock()
	readers := runtime.readers
	runtime.mu.Unlock()
	if readers != 0 {
		t.Fatalf("runtime readers = %d, want detached reader", readers)
	}
}

type deadlineBlockingResponseWriter struct {
	header       http.Header
	blockWrite   bool
	blockFlush   bool
	writeEntered chan struct{}
	flushEntered chan struct{}
	writeOnce    sync.Once
	flushOnce    sync.Once

	mu       sync.Mutex
	deadline time.Time
}

func newDeadlineBlockingResponseWriter(blockWrite, blockFlush bool) *deadlineBlockingResponseWriter {
	return &deadlineBlockingResponseWriter{
		header:       make(http.Header),
		blockWrite:   blockWrite,
		blockFlush:   blockFlush,
		writeEntered: make(chan struct{}),
		flushEntered: make(chan struct{}),
	}
}

func (writer *deadlineBlockingResponseWriter) Header() http.Header { return writer.header }

func (*deadlineBlockingResponseWriter) WriteHeader(int) {}

func (writer *deadlineBlockingResponseWriter) Write(payload []byte) (int, error) {
	if !writer.blockWrite {
		return len(payload), nil
	}
	return 0, writer.waitForDeadline(writer.writeEntered, &writer.writeOnce)
}

func (*deadlineBlockingResponseWriter) Flush() {}

func (writer *deadlineBlockingResponseWriter) FlushError() error {
	if !writer.blockFlush {
		return nil
	}
	return writer.waitForDeadline(writer.flushEntered, &writer.flushOnce)
}

func (writer *deadlineBlockingResponseWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	writer.deadline = deadline
	writer.mu.Unlock()
	return nil
}

func (writer *deadlineBlockingResponseWriter) waitForDeadline(entered chan struct{}, once *sync.Once) error {
	once.Do(func() { close(entered) })
	writer.mu.Lock()
	deadline := writer.deadline
	writer.mu.Unlock()
	if deadline.IsZero() {
		return errors.New("blocking operation started without a write deadline")
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	<-timer.C
	return context.DeadlineExceeded
}
