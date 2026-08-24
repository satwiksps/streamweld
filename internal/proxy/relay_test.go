package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
)

type relayDirectoryStub struct {
	owner journal.OwnerRecord
	err   error
}

func (directory relayDirectoryStub) LocateOwner(context.Context, journal.StreamID) (journal.OwnerRecord, error) {
	return directory.owner, directory.err
}

func (relayDirectoryStub) HeartbeatOwner(context.Context, journal.OwnerRecord, time.Duration) error {
	return nil
}

func TestRemoteRelayForwardsOnlyProtocolFieldsAndAllowlistedResponseHeaders(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	received := make(chan http.Header, 1)
	ownerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received <- request.Header.Clone()
		writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		writer.Header().Set(headerStreamID, id.String())
		writer.Header().Set(headerDurability, durabilityDurable)
		writer.Header().Set("Set-Cookie", "owner-secret=yes")
		writer.Header().Set("X-Streamweld-Owner-Internal", "replica-a")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, "data: [DONE]\n\n")
	}))
	t.Cleanup(ownerServer.Close)

	coordinator := &relayCoordinator{
		replicaID: "replica-b",
		enabled:   true,
		insecure:  true,
		directory: relayDirectoryStub{owner: journal.OwnerRecord{ReplicaID: "replica-a", RelayURL: ownerServer.URL}},
		client:    ownerServer.Client(),
		logger:    slog.New(slog.DiscardHandler),
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/streams/"+id.String()+"/events", nil)
	request.Header.Set("Authorization", "Bearer public-secret")
	request.Header.Set("Cookie", "session=public-secret")
	request.Header.Set(headerIdempotency, "private-key")
	request.Header.Set("X-Untrusted", "do-not-forward")
	recorder := httptest.NewRecorder()
	if handled := coordinator.tryServeRemoteEvents(recorder, request, id, 9, true); !handled {
		t.Fatal("remote owner relay was not used")
	}
	forwarded := <-received
	if forwarded.Get("Last-Event-ID") != "9" || forwarded.Get(headerVerbose) != "1" {
		t.Fatalf("relay protocol headers = %#v", forwarded)
	}
	for _, forbidden := range []string{"Authorization", "Cookie", headerIdempotency, "X-Untrusted", "User-Agent"} {
		if forwarded.Get(forbidden) != "" {
			t.Errorf("relay forwarded forbidden header %s", forbidden)
		}
	}
	response := recorder.Result()
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || recorder.Body.String() != "data: [DONE]\n\n" {
		t.Fatalf("relayed response = %d %q", response.StatusCode, recorder.Body.String())
	}
	if response.Header.Get("Set-Cookie") != "" || response.Header.Get("X-Streamweld-Owner-Internal") != "" {
		t.Fatalf("private relay response headers leaked: %#v", response.Header)
	}
}

func TestRemoteRelayFallsBackBeforeHeadersForUnknownStaleSelfOrUnavailableOwner(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	for _, test := range []struct {
		name      string
		directory relayDirectoryStub
	}{
		{"unknown", relayDirectoryStub{err: journal.ErrOwnerNotRecorded}},
		{"stale", relayDirectoryStub{err: journal.ErrOwnerUnavailable}},
		{"lookup failure", relayDirectoryStub{err: errors.New("Redis unavailable")}},
		{"self", relayDirectoryStub{owner: journal.OwnerRecord{ReplicaID: "replica-b", RelayURL: "http://127.0.0.1:1"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			coordinator := &relayCoordinator{
				replicaID: "replica-b", enabled: true, directory: test.directory,
				client: http.DefaultClient, logger: slog.New(slog.DiscardHandler),
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			if coordinator.tryServeRemoteEvents(recorder, request, id, 0, false) {
				t.Fatal("relay handled request which should fall back")
			}
			if recorder.Code != http.StatusOK || recorder.Body.Len() != 0 || len(recorder.Header()) != 0 {
				t.Fatalf("fallback committed a response: code=%d headers=%#v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
		})
	}
}

func TestRemoteRelayRejectsDirectoryDowngradeAndSSRFBeforeDial(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	for _, test := range []struct {
		name     string
		insecure bool
		relayURL string
	}{
		{"development public host", true, "http://relay.attacker.example:8081"},
		{"development TLS downgrade confusion", true, "https://127.0.0.1:8081"},
		{"production plaintext downgrade", false, "http://127.0.0.1:8081"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var dialed atomic.Bool
			coordinator := &relayCoordinator{
				replicaID: "replica-b", enabled: true, insecure: test.insecure,
				directory: relayDirectoryStub{owner: journal.OwnerRecord{ReplicaID: "replica-a", RelayURL: test.relayURL}},
				client: &http.Client{Transport: relayRoundTripperFunc(func(*http.Request) (*http.Response, error) {
					dialed.Store(true)
					return nil, errors.New("unexpected dial")
				})},
				logger: slog.New(slog.DiscardHandler),
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			if coordinator.tryServeRemoteEvents(recorder, request, id, 0, false) {
				t.Fatal("unsafe directory owner was handled")
			}
			if dialed.Load() {
				t.Fatal("unsafe directory owner was dialed")
			}
			if recorder.Body.Len() != 0 || len(recorder.Header()) != 0 {
				t.Fatalf("unsafe owner fallback committed response: %#v %q", recorder.Header(), recorder.Body.String())
			}
		})
	}
}

func TestRemoteRelayPrivateFailureStatusesFallBackBeforeHeaders(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	for _, status := range []int{
		http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusInternalServerError, http.StatusServiceUnavailable,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ownerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(status)
			}))
			defer ownerServer.Close()
			coordinator := &relayCoordinator{
				replicaID: "replica-b", enabled: true, insecure: true,
				directory: relayDirectoryStub{owner: journal.OwnerRecord{ReplicaID: "replica-a", RelayURL: ownerServer.URL}},
				client:    ownerServer.Client(), logger: slog.New(slog.DiscardHandler),
			}
			recorder := httptest.NewRecorder()
			if coordinator.tryServeRemoteEvents(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil), id, 0, false) {
				t.Fatalf("status %d was relayed instead of falling back", status)
			}
			if recorder.Body.Len() != 0 || len(recorder.Header()) != 0 {
				t.Fatalf("status %d fallback committed response", status)
			}
		})
	}
}

func TestRemoteRelayRejectsUntrustedSuccessAndRedirectResponsesBeforeHeaders(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	validHeaders := func() http.Header {
		header := make(http.Header)
		header.Set("Content-Type", "text/event-stream; charset=utf-8")
		header.Set(headerStreamID, id.String())
		header.Set(headerDurability, durabilityDurable)
		return header
	}
	validCursorError := func() string {
		return `{"error":{"type":"streamweld_error","code":"cursor_ahead","message":"ahead","stream_id":"` + id.String() + `"}}`
	}
	tests := []struct {
		name    string
		status  int
		headers func() http.Header
		body    string
	}{
		{
			name: "wrong stream ID", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header.Set(headerStreamID, "01arz3ndektsv4rrffq69g5faa")
				return header
			},
		},
		{
			name: "missing stream ID", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header.Del(headerStreamID)
				return header
			},
		},
		{
			name: "duplicate stream ID", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header[http.CanonicalHeaderKey(headerStreamID)] = []string{id.String(), id.String()}
				return header
			},
		},
		{
			name: "missing durability", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header.Del(headerDurability)
				return header
			},
		},
		{
			name: "invalid durability", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header.Set(headerDurability, "best-effort")
				return header
			},
		},
		{
			name: "missing content type", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header.Del("Content-Type")
				return header
			},
		},
		{
			name: "invalid content type", status: http.StatusOK, body: "data: [DONE]\n\n",
			headers: func() http.Header {
				header := validHeaders()
				header.Set("Content-Type", "application/json")
				return header
			},
		},
		{name: "misrouted success", status: http.StatusNoContent, headers: validHeaders},
		{name: "redirect", status: http.StatusFound, headers: validHeaders},
		{
			name: "malformed cursor error", status: http.StatusConflict, body: "not-json",
			headers: func() http.Header {
				header := make(http.Header)
				header.Set("Content-Type", "application/json")
				return header
			},
		},
		{
			name: "wrong cursor error stream", status: http.StatusConflict,
			body: strings.Replace(validCursorError(), id.String(), "01arz3ndektsv4rrffq69g5faa", 1),
			headers: func() http.Header {
				header := make(http.Header)
				header.Set("Content-Type", "application/json")
				return header
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &relayTrackingBody{Reader: strings.NewReader(test.body)}
			coordinator := newRelayResponseTestCoordinator(test.status, test.headers(), body)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			if coordinator.tryServeRemoteEvents(recorder, request, id, 0, false) {
				t.Fatal("untrusted relay response was committed")
			}
			assertRelayFallbackUntouched(t, recorder, body)
		})
	}
}

func TestRemoteRelayPreservesValidatedCursorErrors(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	tests := []struct {
		status int
		code   string
	}{
		{http.StatusConflict, "cursor_ahead"},
		{http.StatusGone, "stream_offset_expired"},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			payload := `{"error":{"type":"streamweld_error","code":"` + test.code +
				`","message":"cursor outcome","stream_id":"` + id.String() + `"}}`
			body := &relayTrackingBody{Reader: strings.NewReader(payload)}
			header := make(http.Header)
			header.Set("Content-Type", "application/json; charset=utf-8")
			header.Set("Cache-Control", "no-store")
			coordinator := newRelayResponseTestCoordinator(test.status, header, body)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			if !coordinator.tryServeRemoteEvents(recorder, request, id, 0, false) {
				t.Fatal("validated cursor response was not relayed")
			}
			if recorder.Code != test.status || recorder.Body.String() != payload {
				t.Fatalf("cursor response = %d %q", recorder.Code, recorder.Body.String())
			}
			if !body.closed.Load() {
				t.Fatal("validated cursor response body was not closed")
			}
		})
	}
}

func TestRemoteStopRelayForwardsNoPublicCredentialsAndAllowlistedJSON(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	received := make(chan *http.Request, 1)
	ownerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received <- request.Clone(context.Background())
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.Header().Set("Set-Cookie", "owner-secret=yes")
		writer.Header().Set("X-Streamweld-Owner-Internal", "replica-a")
		writer.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(writer, `{"stream_id":"`+id.String()+`","outcome":"stopped"}`)
	}))
	defer ownerServer.Close()
	coordinator := &relayCoordinator{
		replicaID: "replica-b", enabled: true, insecure: true,
		directory: relayDirectoryStub{owner: journal.OwnerRecord{ReplicaID: "replica-a", RelayURL: ownerServer.URL}},
		client:    ownerServer.Client(), logger: slog.New(slog.DiscardHandler),
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/public/stop", strings.NewReader("public-secret-body"))
	request.Header.Set("Authorization", "Bearer public-secret")
	request.Header.Set("Cookie", "session=public-secret")
	request.Header.Set(headerIdempotency, "private-key")
	recorder := httptest.NewRecorder()
	if !coordinator.tryServeRemoteStop(recorder, request, id) {
		t.Fatal("remote stop owner was not used")
	}
	forwarded := <-received
	if forwarded.Method != http.MethodPost || !strings.HasSuffix(forwarded.URL.Path, "/"+id.String()+"/stop") {
		t.Fatalf("owner stop request = %s %s", forwarded.Method, forwarded.URL.Path)
	}
	for _, forbidden := range []string{"Authorization", "Cookie", headerIdempotency, "User-Agent"} {
		if forwarded.Header.Get(forbidden) != "" {
			t.Errorf("stop relay forwarded forbidden header %s", forbidden)
		}
	}
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"outcome":"stopped"`) {
		t.Fatalf("stop relay response = %d %q", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Set-Cookie") != "" || recorder.Header().Get("X-Streamweld-Owner-Internal") != "" {
		t.Fatalf("private stop response headers leaked: %#v", recorder.Header())
	}
}

func TestRemoteStopRelayRejectsMalformedOrMisroutedResponsesBeforeHeaders(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	validBody := `{"stream_id":"` + id.String() + `","outcome":"stopped"}`
	validHeaders := func() http.Header {
		header := make(http.Header)
		header.Set("Content-Type", "application/json; charset=utf-8")
		return header
	}
	tests := []struct {
		name    string
		status  int
		headers func() http.Header
		body    string
	}{
		{name: "unexpected success", status: http.StatusOK, headers: validHeaders, body: validBody},
		{name: "redirect", status: http.StatusTemporaryRedirect, headers: validHeaders, body: validBody},
		{
			name: "missing content type", status: http.StatusAccepted, body: validBody,
			headers: func() http.Header { return make(http.Header) },
		},
		{
			name: "invalid content type", status: http.StatusAccepted, body: validBody,
			headers: func() http.Header {
				header := make(http.Header)
				header.Set("Content-Type", "text/event-stream")
				return header
			},
		},
		{name: "malformed accepted body", status: http.StatusAccepted, headers: validHeaders, body: "not-json"},
		{name: "missing accepted stream ID", status: http.StatusAccepted, headers: validHeaders, body: `{"outcome":"stopped"}`},
		{
			name: "wrong accepted stream ID", status: http.StatusAccepted, headers: validHeaders,
			body: strings.Replace(validBody, id.String(), "01arz3ndektsv4rrffq69g5faa", 1),
		},
		{
			name: "wrong accepted outcome", status: http.StatusAccepted, headers: validHeaders,
			body: strings.Replace(validBody, "stopped", "running", 1),
		},
		{
			name: "malformed conflict", status: http.StatusConflict, headers: validHeaders,
			body: `{"error":{"type":"streamweld_error","code":"stream_already_terminal","message":"done"}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &relayTrackingBody{Reader: strings.NewReader(test.body)}
			coordinator := newRelayResponseTestCoordinator(test.status, test.headers(), body)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
			if coordinator.tryServeRemoteStop(recorder, request, id) {
				t.Fatal("untrusted stop response was committed")
			}
			assertRelayFallbackUntouched(t, recorder, body)
		})
	}
}

func TestRemoteStopRelayPreservesValidatedStructuredError(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	payload := `{"error":{"type":"streamweld_error","code":"stream_already_terminal",` +
		`"message":"already complete","stream_id":"` + id.String() + `"}}`
	body := &relayTrackingBody{Reader: strings.NewReader(payload)}
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	coordinator := newRelayResponseTestCoordinator(http.StatusConflict, header, body)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	if !coordinator.tryServeRemoteStop(recorder, request, id) {
		t.Fatal("validated stop error was not relayed")
	}
	if recorder.Code != http.StatusConflict || recorder.Body.String() != payload {
		t.Fatalf("stop error response = %d %q", recorder.Code, recorder.Body.String())
	}
}

func TestRelayResponseHeaderWaitIsBounded(t *testing.T) {
	t.Parallel()
	config := relayConfig{
		InsecureDevMode: true, DialTimeout: time.Second, HandshakeTimeout: 75 * time.Millisecond,
	}
	_, transport, err := buildRelayTLS(config)
	if err != nil {
		t.Fatalf("buildRelayTLS() error = %v", err)
	}
	if transport.ResponseHeaderTimeout != config.HandshakeTimeout {
		t.Fatalf("ResponseHeaderTimeout = %s, want %s", transport.ResponseHeaderTimeout, config.HandshakeTimeout)
	}
	transport.CloseIdleConnections()
}

func TestRelayOperationURLSupportsPodIPFamilies(t *testing.T) {
	t.Parallel()
	id := mustRelayStreamID(t)
	for _, base := range []string{"https://10.42.1.17:8081", "https://[fd00::17]:8081"} {
		got, err := relayOperationURL(base, id, "stop")
		if err != nil {
			t.Fatalf("relayOperationURL(%q) error = %v", base, err)
		}
		want := base + relayEventsPrefix + id.String() + "/stop"
		if got != want {
			t.Errorf("relayOperationURL(%q) = %q, want %q", base, got, want)
		}
	}
}

func TestRelayTLSUsesConfiguredServerName(t *testing.T) {
	t.Parallel()

	fixture := httptest.NewTLSServer(http.NotFoundHandler())
	defer fixture.Close()
	pair := fixture.TLS.Certificates[0]
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]})
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatalf("marshal fixture private key: %v", err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatalf("parse fixture certificate: %v", err)
	}
	if len(leaf.DNSNames) == 0 {
		t.Fatal("TLS fixture certificate has no DNS identity")
	}
	directory := t.TempDir()
	caPath := filepath.Join(directory, "ca.crt")
	certificatePath := filepath.Join(directory, "tls.crt")
	privateKeyPath := filepath.Join(directory, "tls.key")
	for path, contents := range map[string][]byte{
		caPath: certificatePEM, certificatePath: certificatePEM, privateKeyPath: privateKeyPEM,
	} {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("write relay TLS fixture: %v", err)
		}
	}

	serverName := leaf.DNSNames[0]
	serverTLS, transport, err := buildRelayTLS(relayConfig{
		CAFile: caPath, CertificateFile: certificatePath, PrivateKeyFile: privateKeyPath,
		TLSServerName: serverName, DialTimeout: time.Second, HandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("buildRelayTLS() error = %v", err)
	}
	if serverTLS == nil || transport.TLSClientConfig == nil {
		t.Fatal("buildRelayTLS() omitted production TLS configuration")
	}
	if got := transport.TLSClientConfig.ServerName; got != serverName {
		t.Fatalf("relay TLS ServerName = %q, want %q", got, serverName)
	}
	response, err := (&http.Client{Transport: transport}).Get(fixture.URL)
	if err != nil {
		t.Fatalf("relay TLS handshake through IP with server name %q: %v", serverName, err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close relay TLS fixture response: %v", err)
	}
	transport.CloseIdleConnections()

	_, wrongNameTransport, err := buildRelayTLS(relayConfig{
		CAFile: caPath, CertificateFile: certificatePath, PrivateKeyFile: privateKeyPath,
		TLSServerName: "wrong.example.test", DialTimeout: time.Second, HandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("buildRelayTLS() with wrong name error = %v", err)
	}
	if response, err := (&http.Client{Transport: wrongNameTransport}).Get(fixture.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("relay TLS handshake accepted a certificate for the wrong server name")
	}
	wrongNameTransport.CloseIdleConnections()
}

func TestRelayTLSVerificationGate(t *testing.T) {
	t.Parallel()
	config := &tls.Config{}
	plain := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	if relayTLSVerified(plain, config) {
		t.Fatal("request without TLS passed relay verification")
	}
	plain.TLS = &tls.ConnectionState{}
	if relayTLSVerified(plain, config) {
		t.Fatal("request without a verified client chain passed relay verification")
	}
	plain.TLS.VerifiedChains = [][]*x509.Certificate{{{}}}
	if !relayTLSVerified(plain, config) {
		t.Fatal("verified mutual-TLS request was rejected")
	}
	if !relayTLSVerified(httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil), nil) {
		t.Fatal("explicit insecure development request was rejected")
	}
}

func TestRelayShutdownJoinsHeartbeatAndIsIdempotent(t *testing.T) {
	directory := &relayLifecycleDirectory{
		backgroundEntered: make(chan struct{}),
		backgroundExited:  make(chan struct{}),
	}
	coordinator := &relayCoordinator{
		replicaID: "replica-a",
		owner: journal.OwnerRecord{
			ReplicaID: "replica-a",
			RelayURL:  "http://127.0.0.1:18081",
		},
		directory: directory,
		logger:    slog.New(slog.DiscardHandler),
		server:    &http.Server{Handler: http.NotFoundHandler()},
		listen:    "127.0.0.1:0",
		heartbeat: 100 * time.Millisecond,
		ttl:       time.Second,
		enabled:   true,
		insecure:  true,
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	serveResult, err := coordinator.start(serveContext)
	if err != nil {
		t.Fatalf("relay start error = %v", err)
	}
	select {
	case <-directory.backgroundEntered:
	case <-time.After(time.Second):
		t.Fatal("background heartbeat did not start")
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	if err := coordinator.shutdown(shutdownContext); err != nil {
		t.Fatalf("first relay shutdown error = %v", err)
	}
	select {
	case <-directory.backgroundExited:
	default:
		t.Fatal("relay shutdown returned before the active heartbeat exited")
	}
	coordinator.mu.Lock()
	heartbeatDone := coordinator.heartbeatDone
	coordinator.mu.Unlock()
	select {
	case <-heartbeatDone:
	default:
		t.Fatal("relay shutdown returned before the heartbeat goroutine joined")
	}
	select {
	case serveErr := <-serveResult:
		if serveErr != nil {
			t.Fatalf("relay Serve result = %v", serveErr)
		}
	case <-time.After(time.Second):
		t.Fatal("relay server did not stop accepting after shutdown")
	}

	if err := coordinator.shutdown(shutdownContext); err != nil {
		t.Fatalf("second relay shutdown error = %v", err)
	}
	callsAfterShutdown := directory.calls.Load()
	time.Sleep(2 * coordinator.heartbeat)
	if calls := directory.calls.Load(); calls != callsAfterShutdown {
		t.Fatalf("heartbeat calls advanced from %d to %d after shutdown returned", callsAfterShutdown, calls)
	}
}

func TestCopyRelayResponseHeadersDoesNotAccumulateOrLeak(t *testing.T) {
	t.Parallel()
	source := http.Header{
		"Content-Type":                {"text/event-stream"},
		"Cache-Control":               {"no-store"},
		"Set-Cookie":                  {"secret=yes"},
		"X-Streamweld-Owner-Internal": {"replica-a"},
	}
	destination := http.Header{"Content-Type": {"old"}}
	copyRelayResponseHeaders(destination, source)
	if got := destination.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q", got)
	}
	if serialized := destination.Get("Set-Cookie") + destination.Get("X-Streamweld-Owner-Internal"); strings.TrimSpace(serialized) != "" {
		t.Fatalf("private headers copied: %#v", destination)
	}
}

func mustRelayStreamID(t *testing.T) journal.StreamID {
	t.Helper()
	id, err := journal.ParseStreamID("01arz3ndektsv4rrffq69g5fav")
	if err != nil {
		t.Fatalf("ParseStreamID() error = %v", err)
	}
	return id
}

func newRelayResponseTestCoordinator(
	status int,
	header http.Header,
	body io.ReadCloser,
) *relayCoordinator {
	return &relayCoordinator{
		replicaID: "replica-b",
		enabled:   true,
		insecure:  true,
		directory: relayDirectoryStub{owner: journal.OwnerRecord{
			ReplicaID: "replica-a",
			RelayURL:  "http://127.0.0.1:18081",
		}},
		client: &http.Client{
			Transport: relayRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: status,
					Header:     header,
					Body:       body,
					Request:    request,
				}, nil
			}),
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: slog.New(slog.DiscardHandler),
	}
}

func assertRelayFallbackUntouched(t *testing.T, recorder *httptest.ResponseRecorder, body *relayTrackingBody) {
	t.Helper()
	if recorder.Body.Len() != 0 || len(recorder.Header()) != 0 {
		t.Fatalf("relay fallback committed a response: headers=%#v body=%q", recorder.Header(), recorder.Body.String())
	}
	if !body.closed.Load() {
		t.Fatal("rejected relay response body was not closed")
	}
	recorder.Header().Set("X-Streamweld-Test-Fallback", "redis")
	recorder.WriteHeader(http.StatusTeapot)
	_, _ = io.WriteString(recorder, "redis fallback")
	if recorder.Code != http.StatusTeapot || recorder.Body.String() != "redis fallback" {
		t.Fatalf("fallback could not write response: code=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

type relayTrackingBody struct {
	io.Reader
	closed atomic.Bool
}

type relayLifecycleDirectory struct {
	calls             atomic.Int32
	backgroundEntered chan struct{}
	backgroundExited  chan struct{}
	enteredOnce       sync.Once
	exitedOnce        sync.Once
}

func (*relayLifecycleDirectory) LocateOwner(context.Context, journal.StreamID) (journal.OwnerRecord, error) {
	return journal.OwnerRecord{}, journal.ErrOwnerNotRecorded
}

func (directory *relayLifecycleDirectory) HeartbeatOwner(
	ctx context.Context,
	_ journal.OwnerRecord,
	_ time.Duration,
) error {
	if directory.calls.Add(1) == 1 {
		return nil
	}
	directory.enteredOnce.Do(func() { close(directory.backgroundEntered) })
	<-ctx.Done()
	directory.exitedOnce.Do(func() { close(directory.backgroundExited) })
	return ctx.Err()
}

func (body *relayTrackingBody) Close() error {
	body.closed.Store(true)
	return nil
}

type relayRoundTripperFunc func(*http.Request) (*http.Response, error)

func (roundTrip relayRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}
