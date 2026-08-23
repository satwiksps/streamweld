package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/conformance"
)

func TestBackendDrainRequiresConfiguredAdminBearer(t *testing.T) {
	t.Parallel()
	address, err := url.Parse("http://backend:8000")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := backend.NewPool(backend.DefaultConfig(), backend.Backend{
		ID: "backend:8000", URL: address, TemplateVerdict: conformance.VerdictSafe,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{
		durable:    &durableService{backends: pool},
		adminToken: "secret-token",
	}
	target := "/internal/backends/" + url.PathEscape("backend:8000") + "/drain?timeout=100ms"

	for _, test := range []struct {
		name          string
		authorization string
	}{
		{name: "missing"},
		{name: "wrong", authorization: "Bearer wrong-token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, target, nil)
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("response = %d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
			}
			state, stateErr := pool.Get("backend:8000")
			if stateErr != nil || state.Draining {
				t.Fatalf("unauthorized drain mutated backend = (%+v, %v)", state, stateErr)
			}
		})
	}

	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, target, nil)
	request.Header.Set("Authorization", "Bearer secret-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authorized drain response = %d, body=%s", response.Code, response.Body.String())
	}
	state, err := pool.Get("backend:8000")
	if err != nil || !state.Draining {
		t.Fatalf("authorized drain backend state = (%+v, %v)", state, err)
	}
}
