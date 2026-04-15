package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestJournalDegradedDoesNotClearWhileAnotherStreamIsDegraded(t *testing.T) {
	recorder, err := New(nil, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	labels := Labels{Route: "team-a/chat", Model: "model-a"}
	recorder.StreamStarted(labels)
	recorder.StreamStarted(labels)
	recorder.JournalDegraded(labels, "degraded", true)
	recorder.JournalDegraded(labels, "healthy", false)
	if got := testutil.ToFloat64(recorder.journalDegraded.WithLabelValues(labels.Route, labels.Model)); got != 1 {
		t.Fatalf("journal degraded after another stream succeeded = %v, want 1", got)
	}

	recorder.StreamFinished(labels, "degraded", "done", time.Second)
	if got := testutil.ToFloat64(recorder.journalDegraded.WithLabelValues(labels.Route, labels.Model)); got != 0 {
		t.Fatalf("journal degraded after recovery and degraded stream completion = %v, want 0", got)
	}
	recorder.StreamFinished(labels, "healthy", "done", time.Second)
}

func TestJournalDegradedStaysSetUntilAHealthyOperation(t *testing.T) {
	recorder, err := New(nil, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	labels := Labels{Model: "model-a"}
	recorder.StreamStarted(labels)
	recorder.JournalDegraded(labels, "failed", true)
	recorder.StreamFinished(labels, "failed", "error", time.Second)
	if got := testutil.ToFloat64(recorder.journalDegraded.WithLabelValues(DefaultRoute, "model-a")); got != 1 {
		t.Fatalf("journal degraded before recovery = %v, want 1", got)
	}
	recorder.JournalDegraded(labels, "probe", false)
	if got := testutil.ToFloat64(recorder.journalDegraded.WithLabelValues(DefaultRoute, "model-a")); got != 0 {
		t.Fatalf("journal degraded after recovery = %v, want 0", got)
	}
}

func TestNewOTLPTraceProviderExportsOverHTTP(t *testing.T) {
	received := make(chan struct{}, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/custom/traces" {
			t.Errorf("OTLP path = %q", request.URL.Path)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read OTLP body: %v", err)
		}
		if len(body) == 0 {
			t.Error("OTLP request body is empty")
		}
		received <- struct{}{}
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	provider, err := NewOTLPTraceProvider(ctx, collector.URL+"/custom/traces", "streamweld-test", "v0")
	if err != nil {
		t.Fatalf("NewOTLPTraceProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("test").Start(ctx, "stream")
	span.End()
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush() error = %v", err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("OTLP collector did not receive a trace")
	}
}

func TestNewOTLPTraceProviderValidatesRequiredInputs(t *testing.T) {
	//nolint:staticcheck // This call verifies the provider's defensive nil-context validation.
	if _, err := NewOTLPTraceProvider(nil, "http://collector", "proxy", "v0"); err == nil {
		t.Error("nil context was accepted")
	}
	if _, err := NewOTLPTraceProvider(context.Background(), "", "proxy", "v0"); err == nil {
		t.Error("empty endpoint was accepted")
	}
	if _, err := NewOTLPTraceProvider(context.Background(), "://bad", "proxy", "v0"); err == nil {
		t.Error("invalid endpoint was accepted")
	}
	if got, err := exactOTLPTraceEndpoint("http://collector:4318"); err != nil || got != "http://collector:4318/" {
		t.Errorf("exact root endpoint = %q, %v", got, err)
	}
}
