package streamweld_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestDashboardCoversEveryProxyMetricAndChartWiresIt(t *testing.T) {
	dashboardBytes, err := os.ReadFile("dashboards/streamweld.json")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	var dashboard any
	if err := json.Unmarshal(dashboardBytes, &dashboard); err != nil {
		t.Fatalf("dashboard JSON is invalid: %v", err)
	}
	dashboardText := string(dashboardBytes)
	metrics := []string{
		"streamweld_streams_active",
		"streamweld_streams_total",
		"streamweld_migrations_total",
		"streamweld_migrations_refused_total",
		"streamweld_tokens_rescued_total",
		"streamweld_prompt_tokens_rebilled_total",
		"streamweld_resumes_total",
		"streamweld_seam_overlap_bytes",
		"streamweld_ttft_seconds",
		"streamweld_inter_token_seconds",
		"streamweld_stream_duration_seconds",
		"streamweld_journal_bytes",
		"streamweld_journal_degraded",
		"streamweld_backends",
	}
	for _, metric := range metrics {
		if !strings.Contains(dashboardText, metric) {
			t.Errorf("dashboard does not query %s", metric)
		}
	}
	if !strings.Contains(dashboardText, `"name": "route"`) ||
		!strings.Contains(dashboardText, `"name": "model"`) {
		t.Error("dashboard does not define route and model variables")
	}

	dashboardTemplate, err := os.ReadFile("templates/grafana-dashboard.yaml")
	if err != nil {
		t.Fatalf("read dashboard template: %v", err)
	}
	if !strings.Contains(string(dashboardTemplate), `.Files.Get "dashboards/streamweld.json"`) {
		t.Error("Helm template does not package the checked dashboard")
	}
	monitorTemplate, err := os.ReadFile("templates/servicemonitor.yaml")
	if err != nil {
		t.Fatalf("read ServiceMonitor template: %v", err)
	}
	if !strings.Contains(string(monitorTemplate), "path: /metrics") {
		t.Error("ServiceMonitor does not scrape the proxy metrics route")
	}
	proxyConfigTemplate, err := os.ReadFile("templates/proxy-configmap.yaml")
	if err != nil {
		t.Fatalf("read proxy ConfigMap template: %v", err)
	}
	if !strings.Contains(string(proxyConfigTemplate), "OTEL_EXPORTER_OTLP_ENDPOINT") ||
		!strings.Contains(string(proxyConfigTemplate), ".Values.observability.tracing.otlpEndpoint") {
		t.Error("proxy ConfigMap does not wire the OTLP tracing endpoint")
	}
}
