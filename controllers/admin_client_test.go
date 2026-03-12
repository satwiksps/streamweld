package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/streamweld/streamweld/internal/conformance"
	"k8s.io/apimachinery/pkg/types"
)

func TestHTTPAdminClientAppliesDeterministicAuthenticatedSnapshot(t *testing.T) {
	t.Parallel()
	const token = "top-secret-value"
	var received BackendSnapshot
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", request.Method)
		}
		if request.URL.EscapedPath() != "/internal/routes/team-a%2Fllama/backends" {
			t.Errorf("escaped path = %q", request.URL.EscapedPath())
		}
		if got := request.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("authorization = %q", got)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"route":"team-a/llama","model":"llama","applied_generation":7,"backend_count":2,"draining_backends":1,"draining_backend_ids":["retired/a"],"active_streams":3}`)
	}))
	defer server.Close()

	client, err := NewHTTPAdminClient(AdminClientConfig{
		BaseURL: server.URL, BearerToken: token, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Apply(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 7, UID: "route-uid", Policy: testAdminPolicy(),
		Backends: []BackendRegistration{
			{ID: "z", URL: "http://10.0.0.2:8000", TemplateVerdict: conformance.VerdictUnsafe, PodNamespace: "team-a", PodName: "pod-z"},
			{ID: "a", URL: "http://10.0.0.1:8000", TemplateVerdict: conformance.VerdictSafe, PodNamespace: "team-a", PodName: "pod-a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BackendCount != 2 || result.DrainingBackends != 1 || result.ActiveStreams != 3 ||
		len(result.DrainingBackendIDs) != 1 || result.DrainingBackendIDs[0] != "retired/a" {
		t.Fatalf("result = %#v", result)
	}
	if len(received.Backends) != 2 || received.Backends[0].ID != "a" || received.Backends[1].ID != "z" {
		t.Fatalf("backends were not deterministically sorted: %#v", received.Backends)
	}
}

func TestValidateDrainingBackendIDsRequiresBoundedCanonicalCompleteSet(t *testing.T) {
	t.Parallel()
	tooMany := make([]string, maxAdminDrainingBackendIDs+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("retired/%03d", index)
	}
	tests := []struct {
		name   string
		result AdminResult
		ok     bool
	}{
		{name: "legacy count only", result: AdminResult{DrainingBackends: 3}, ok: true},
		{name: "canonical complete", result: AdminResult{DrainingBackends: 2, DrainingBackendIDs: []string{"retired/a", "retired/b"}}, ok: true},
		{name: "incomplete", result: AdminResult{DrainingBackends: 2, DrainingBackendIDs: []string{"retired/a"}}},
		{name: "unsorted", result: AdminResult{DrainingBackends: 2, DrainingBackendIDs: []string{"retired/b", "retired/a"}}},
		{name: "duplicate", result: AdminResult{DrainingBackends: 2, DrainingBackendIDs: []string{"retired/a", "retired/a"}}},
		{name: "too many", result: AdminResult{DrainingBackends: int32(len(tooMany)), DrainingBackendIDs: tooMany}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateDrainingBackendIDs(test.result)
			if (err == nil) != test.ok {
				t.Fatalf("error = %v, ok = %t", err, test.ok)
			}
		})
	}
}

func TestHTTPAdminClientReturnsBoundedStaleResult(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(writer, `{"route":"ns/route","model":"m","applied_generation":9,"backend_count":1,"draining_backends":0,"active_streams":2,"error":{"message":"private details"}}`)
	}))
	defer server.Close()
	client, err := NewHTTPAdminClient(AdminClientConfig{BaseURL: server.URL, BearerToken: "token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Apply(context.Background(), types.NamespacedName{Namespace: "ns", Name: "route"}, BackendSnapshot{
		Model: "m", ObservedGeneration: 8, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("error = %v, want ErrStaleSnapshot", err)
	}
	if result.AppliedGeneration != 9 || strings.Contains(err.Error(), "private") {
		t.Fatalf("result/error leaked or lost data: %#v, %v", result, err)
	}
}

func TestHTTPAdminClientSecurityAndBounds(t *testing.T) {
	t.Parallel()
	t.Run("rejects plaintext remote URL", func(t *testing.T) {
		_, err := NewHTTPAdminClient(AdminClientConfig{BaseURL: "http://proxy.default.svc:8080", BearerToken: "token"})
		if !errors.Is(err, ErrInvalidAdminConfig) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("loopback development is explicit", func(t *testing.T) {
		_, err := NewHTTPAdminClient(AdminClientConfig{
			BaseURL: "http://127.0.0.1:8080", AllowInsecureHTTP: true, AllowUnauthenticatedLoopback: true,
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("does not follow redirects", func(t *testing.T) {
		var followed bool
		destination := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
		defer destination.Close()
		source := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			http.Redirect(writer, request, destination.URL, http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		client, err := NewHTTPAdminClient(AdminClientConfig{BaseURL: source.URL, BearerToken: "token", HTTPClient: source.Client()})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Apply(context.Background(), types.NamespacedName{Namespace: "ns", Name: "route"}, BackendSnapshot{Model: "m", ObservedGeneration: 1, UID: "route-uid", Policy: testAdminPolicy()})
		if err == nil || followed {
			t.Fatalf("error = %v, followed = %v", err, followed)
		}
	})
	t.Run("bounds response", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(writer, strings.Repeat("x", maxAdminResponseBytes+1))
		}))
		defer server.Close()
		client, err := NewHTTPAdminClient(AdminClientConfig{BaseURL: server.URL, BearerToken: "token", HTTPClient: server.Client()})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Apply(context.Background(), types.NamespacedName{Namespace: "ns", Name: "route"}, BackendSnapshot{Model: "m", ObservedGeneration: 1, UID: "route-uid", Policy: testAdminPolicy()})
		if err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestNormalizeSnapshotRejectsUnprobedAndDuplicateBackends(t *testing.T) {
	t.Parallel()
	base := BackendRegistration{ID: "a", URL: "http://10.0.0.1:8000", TemplateVerdict: conformance.VerdictUnknown}
	_, err := normalizeSnapshot(BackendSnapshot{Model: "m", ObservedGeneration: 1, UID: "route-uid", Policy: testAdminPolicy(), Backends: []BackendRegistration{base}})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("unknown verdict error = %v", err)
	}
	base.TemplateVerdict = conformance.VerdictSafe
	_, err = normalizeSnapshot(BackendSnapshot{Model: "m", ObservedGeneration: 1, UID: "route-uid", Policy: testAdminPolicy(), Backends: []BackendRegistration{base, base}})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("duplicate error = %v", err)
	}
}

func testAdminPolicy() AdminRoutePolicy {
	return AdminRoutePolicy{
		MaxMigrations: 3, MaxMigrationTokens: 8192, MaxStreamDuration: "15m",
		OrphanPolicy: "continue", OrphanTimeout: "1m", SeamWindowBytes: 64, JournalTTL: "10m",
	}
}
