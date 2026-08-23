package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/satwiksps/streamweld/internal/journal"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

type blockingOrphanClaimJournal struct {
	*journal.Redis

	claimReached chan struct{}
	allowClaim   chan struct{}
	reachedOnce  sync.Once
	allowOnce    sync.Once
}

func newBlockingOrphanClaimJournal(store *journal.Redis) *blockingOrphanClaimJournal {
	return &blockingOrphanClaimJournal{
		Redis:        store,
		claimReached: make(chan struct{}),
		allowClaim:   make(chan struct{}),
	}
}

func (store *blockingOrphanClaimJournal) TryClaimOrphan(
	ctx context.Context,
	id journal.StreamID,
	claimID string,
	ttl time.Duration,
) (bool, error) {
	claimed, err := store.Redis.TryClaimOrphan(ctx, id, claimID, ttl)
	if err != nil || !claimed {
		return claimed, err
	}
	store.reachedOnce.Do(func() { close(store.claimReached) })
	select {
	case <-store.allowClaim:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (store *blockingOrphanClaimJournal) allow() {
	store.allowOnce.Do(func() { close(store.allowClaim) })
}

func TestOrphanFenceReaderAttachedFirstPreventsCancellation(t *testing.T) {
	fixture := newOrphanFenceFixture(t)
	// Each test closes the initial stream at its deliberate race boundary.
	//nolint:bodyclose
	initial, streamID := fixture.open(t)

	resume := newDurableHTTPRequest(t, http.MethodGet,
		fixture.replica.url+"/v1/streams/"+streamID.String()+"/events", "")
	resume.Header.Set("Last-Event-ID", "2")
	// Kept open until the cancellation ordering assertions below.
	//nolint:bodyclose
	resumeResponse := doDurableHTTPRequest(t, fixture.replica.client, resume)
	if resumeResponse.StatusCode != http.StatusOK {
		closeDurableHTTPBody(t, initial.Body)
		t.Fatalf("resume status = %d, want 200", resumeResponse.StatusCode)
	}

	closeDurableHTTPBody(t, initial.Body)
	select {
	case <-fixture.journal.claimReached:
		closeDurableHTTPBody(t, resumeResponse.Body)
		t.Fatal("orphan cancellation claimed while a local reader was attached")
	case <-fixture.producerCanceled:
		closeDurableHTTPBody(t, resumeResponse.Body)
		t.Fatal("producer canceled while a local reader was attached")
	case <-time.After(250 * time.Millisecond):
	}

	closeDurableHTTPBody(t, resumeResponse.Body)
	select {
	case <-fixture.journal.claimReached:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("orphan cancellation did not attempt a fence after the final reader detached")
	}
	select {
	case <-fixture.producerCanceled:
		t.Fatal("producer canceled before the distributed orphan claim completed")
	default:
	}
	fixture.journal.allow()
	select {
	case <-fixture.producerCanceled:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("producer was not canceled after the orphan claim won")
	}
}

func TestOrphanFenceClaimFirstRejectsLateLocalReader(t *testing.T) {
	fixture := newOrphanFenceFixture(t)
	// Closed immediately below to trigger the orphan path.
	//nolint:bodyclose
	initial, streamID := fixture.open(t)
	closeDurableHTTPBody(t, initial.Body)

	select {
	case <-fixture.journal.claimReached:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("orphan cancellation did not reach its distributed fence")
	}
	responseResult := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	go func() {
		request := newDurableHTTPRequest(t, http.MethodGet,
			fixture.replica.url+"/v1/streams/"+streamID.String()+"/events", "")
		request.Header.Set("Last-Event-ID", "2")
		// Ownership transfers to the receiving test goroutine.
		//nolint:bodyclose
		response, err := fixture.replica.client.Do(request)
		responseResult <- struct {
			response *http.Response
			err      error
		}{response: response, err: err}
	}()

	select {
	case result := <-responseResult:
		if result.response != nil {
			closeDurableHTTPBody(t, result.response.Body)
		}
		t.Fatalf("late reader completed before claim result was linearized: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	fixture.journal.allow()
	result := <-responseResult
	if result.err != nil {
		t.Fatalf("late reader request error = %v", result.err)
	}
	defer closeDurableHTTPBody(t, result.response.Body)
	if result.response.StatusCode != http.StatusConflict {
		t.Fatalf("late reader status = %d, want 409", result.response.StatusCode)
	}
	body, err := io.ReadAll(result.response.Body)
	if err != nil {
		t.Fatalf("read late reader response: %v", err)
	}
	if !strings.Contains(string(body), `"code":"stream_closing"`) {
		t.Fatalf("late reader response = %s, want stream_closing", body)
	}
	select {
	case <-fixture.producerCanceled:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("producer was not canceled after the claim won")
	}
}

type orphanFenceFixture struct {
	replica          *durableHTTPHarness
	journal          *blockingOrphanClaimJournal
	producerCanceled <-chan struct{}
}

func newOrphanFenceFixture(t *testing.T) *orphanFenceFixture {
	t.Helper()
	producerCanceled := make(chan struct{})
	var canceledOnce sync.Once
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		<-request.Context().Done()
		canceledOnce.Do(func() { close(producerCanceled) })
	}))
	t.Cleanup(backend.Close)

	redisServer := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	redisConfig := journal.DefaultRedisConfig()
	redisConfig.Prefix = "streamweld:orphan-fence"
	redisConfig.ReadBlock = 5 * time.Millisecond
	redisStore, err := journal.NewRedis(client, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis() error = %v", err)
	}
	blockingStore := newBlockingOrphanClaimJournal(redisStore)
	t.Cleanup(blockingStore.allow)

	config := DefaultConfig()
	config.BackendURL = backend.URL
	config.ListenAddress = "127.0.0.1:0"
	config.OrphanPolicy = OrphanCancel
	config.ReadinessTimeout = 2 * time.Second
	server, err := NewServer(
		config,
		nil,
		WithJournal(blockingStore),
		WithIdempotencyRegistry(redisStore),
		WithStreamIDGenerator(&durableSequentialIDs{}),
	)
	if err != nil {
		t.Fatalf("NewServer(orphan fence) error = %v", err)
	}
	front := httptest.NewServer(server.Handler())
	transport := &http.Transport{DisableCompression: true}
	httpClient := &http.Client{Transport: transport, Timeout: durableHTTPTestTimeout}
	t.Cleanup(func() {
		server.forceCancel()
		front.CloseClientConnections()
		front.Close()
		transport.CloseIdleConnections()
		server.closeIdleConnections()
	})
	return &orphanFenceFixture{
		replica:          &durableHTTPHarness{server: server, url: front.URL, client: httpClient},
		journal:          blockingStore,
		producerCanceled: producerCanceled,
	}
}

func (fixture *orphanFenceFixture) open(t *testing.T) (*http.Response, journal.StreamID) {
	t.Helper()
	request := newDurableHTTPRequest(t, http.MethodPost,
		fixture.replica.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`)
	// Closed explicitly by each test at the intended interleaving.
	//nolint:bodyclose
	response := doDurableHTTPRequest(t, fixture.replica.client, request)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("initial status = %d, want 200", response.StatusCode)
	}
	streamID := requireDurableResponseHeaders(t, response)
	decoder := sse.NewDecoder(response.Body)
	_ = readNextDurableSSE(t, decoder)
	_ = readNextDurableSSE(t, decoder)
	return response, streamID
}
