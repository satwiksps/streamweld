package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestPhase5RemoteReaderLeasePreventsOwnerOrphanCancellation(t *testing.T) {
	producerCanceled := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		<-request.Context().Done()
		close(producerCanceled)
	}))
	t.Cleanup(backend.Close)

	redisServer := miniredis.RunT(t)
	clientA := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	clientB := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	redisConfig := journal.DefaultRedisConfig()
	redisConfig.Prefix = "streamweld:phase5:reader-leases"
	redisConfig.ReadBlock = 5 * time.Millisecond
	storeA, err := journal.NewRedis(clientA, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis(A) error = %v", err)
	}
	storeB, err := journal.NewRedis(clientB, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis(B) error = %v", err)
	}
	replicaA := newPhase5OrphanReplica(t, backend.URL, storeA, OrphanCancel)
	replicaB := newPhase5OrphanReplica(t, backend.URL, storeB, OrphanCancel)

	initial := newDurableHTTPRequest(t, http.MethodPost, replicaA.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`)
	// This test deliberately keeps the body open until the remote reader lease
	// is visible, then closes it at a precise point below.
	//nolint:bodyclose
	initialResponse := doDurableHTTPRequest(t, replicaA.client, initial)
	if initialResponse.StatusCode != http.StatusOK {
		t.Fatalf("initial status = %d", initialResponse.StatusCode)
	}
	streamID := requireDurableResponseHeaders(t, initialResponse)
	decoder := sse.NewDecoder(initialResponse.Body)
	_ = readNextDurableSSE(t, decoder)
	_ = readNextDurableSSE(t, decoder)

	resume := newDurableHTTPRequest(t, http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String()+"/events", "")
	resume.Header.Set("Last-Event-ID", "2")
	// This body is likewise closed explicitly after the orphan-policy check.
	//nolint:bodyclose
	resumeResponse := doDurableHTTPRequest(t, replicaB.client, resume)
	if resumeResponse.StatusCode != http.StatusOK {
		closeDurableHTTPBody(t, initialResponse.Body)
		t.Fatalf("remote resume status = %d", resumeResponse.StatusCode)
	}

	deadline := time.Now().Add(durableHTTPTestTimeout)
	for {
		count, countErr := storeA.ActiveReaders(context.Background(), streamID)
		if countErr == nil && count == 1 {
			break
		}
		if time.Now().After(deadline) {
			closeDurableHTTPBody(t, initialResponse.Body)
			closeDurableHTTPBody(t, resumeResponse.Body)
			t.Fatalf("remote reader lease was not visible: count=%d error=%v", count, countErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	closeDurableHTTPBody(t, initialResponse.Body)
	select {
	case <-producerCanceled:
		closeDurableHTTPBody(t, resumeResponse.Body)
		t.Fatal("owner canceled producer while replica B still had an attached reader")
	case <-time.After(300 * time.Millisecond):
	}

	closeDurableHTTPBody(t, resumeResponse.Body)
	select {
	case <-producerCanceled:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("owner did not cancel producer after the final remote reader detached")
	}
}

func newPhase5OrphanReplica(
	t *testing.T,
	backendURL string,
	store *journal.Redis,
	policy OrphanPolicy,
) *durableHTTPHarness {
	t.Helper()
	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	config.OrphanPolicy = policy
	config.ReadinessTimeout = time.Second
	server, err := NewServer(
		config,
		nil,
		WithJournal(store),
		WithIdempotencyRegistry(store),
		WithStreamIDGenerator(&durableSequentialIDs{}),
	)
	if err != nil {
		t.Fatalf("NewServer(orphan replica) error = %v", err)
	}
	front := httptest.NewServer(server.Handler())
	transport := &http.Transport{DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: durableHTTPTestTimeout}
	t.Cleanup(func() {
		server.forceCancel()
		front.CloseClientConnections()
		front.Close()
		transport.CloseIdleConnections()
		server.closeIdleConnections()
	})
	return &durableHTTPHarness{server: server, url: front.URL, client: client}
}
