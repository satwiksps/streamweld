package streamweld_test

import (
	"os"
	"strings"
	"testing"
)

// Helm 4 represents larger values loaded through values.schema.json as
// float64. Passing those values directly to quote emits scientific notation,
// which the proxy's integer environment parser correctly rejects. Keep every
// integer-valued proxy environment variable explicitly decimal-formatted.
func TestProxyConfigMapFormatsIntegerEnvironmentValuesAsDecimal(t *testing.T) {
	templateBytes, err := os.ReadFile("templates/proxy-configmap.yaml")
	if err != nil {
		t.Fatalf("read proxy ConfigMap template: %v", err)
	}
	template := string(templateBytes)

	values := []string{
		".Values.journal.maxBytesPerStream",
		".Values.journal.maxTotalBytes",
		".Values.reader.maxLagBytes",
		".Values.migration.maxMigrations",
		".Values.migration.maxTokens",
		".Values.migration.seamWindowBytes",
	}
	for _, value := range values {
		line := lineContaining(template, value)
		if !strings.Contains(line, `printf "%d"`) || !strings.Contains(line, "| quote") {
			t.Errorf("%s is not rendered as a quoted decimal integer: %q", value, line)
		}
	}
}

func lineContaining(text, value string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, value) {
			return line
		}
	}
	return ""
}
