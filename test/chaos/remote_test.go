package chaos

import (
	"strings"
	"testing"

	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestAttachedStreamObservesMigrationSeamAndPromptRebilling(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{id: "stream-observed"}
	stream.output.WriteString("token-000 token-001 ")
	if err := stream.accept(sse.Event{
		Type:    "streamweld.stream.migration",
		Data:    []byte(`{"rescued_tokens":2}`),
		HasType: true,
		HasData: true,
	}); err != nil {
		t.Fatalf("accept migration: %v", err)
	}
	if err := stream.accept(sse.Event{
		Data: []byte(`{
			"choices":[{"index":0,"delta":{"content":""}}],
			"usage":{"prompt_tokens":16},
			"streamweld_chaos_raw_delta":"token-001 "
		}`),
		HasData: true,
	}); err != nil {
		t.Fatalf("accept continuation seam: %v", err)
	}
	if stream.migrated != 1 || stream.rescued != 2 || stream.promptRebilled != 16 ||
		len(stream.seamOverlaps) != 1 || stream.seamOverlaps[0] != len("token-001 ") {
		t.Fatalf("observed migration evidence = %+v", stream)
	}
	if got := stream.output.String(); got != "token-000 token-001 " {
		t.Fatalf("seam-reconciled output = %q", got)
	}
}

func TestAttachedStreamRejectsUnobservedMigrationEvidence(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{id: "stream-missing"}
	stream.output.WriteString("token-000 ")
	if err := stream.accept(sse.Event{
		Type:    "streamweld.stream.migration",
		Data:    []byte(`{"rescued_tokens":1}`),
		HasType: true,
		HasData: true,
	}); err != nil {
		t.Fatalf("accept migration: %v", err)
	}
	err := stream.accept(sse.Event{
		Data:    []byte(`{"choices":[{"delta":{"content":"token-001 "}}]}`),
		HasData: true,
	})
	if err == nil || !strings.Contains(err.Error(), "omitted observed seam or prompt-usage metadata") {
		t.Fatalf("missing evidence error = %v", err)
	}
}

func TestAttachedStreamObservesReaderLagEviction(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{id: "stream-slow"}
	err := stream.accept(sse.Event{
		Type:    "streamweld.reader.error",
		Data:    []byte(`{"code":"reader_lag_exceeded"}`),
		HasType: true,
		HasData: true,
	})
	if err != nil || !stream.readerLagged {
		t.Fatalf("reader lag observation = (%t, %v)", stream.readerLagged, err)
	}
}

func TestCompletionTerminalMustMatchScenarioOutcome(t *testing.T) {
	t.Parallel()

	redis, ok := definitionFor(ScenarioRedisDown)
	if !ok {
		t.Fatal("redis-down definition is missing")
	}
	canonical := "token-000 token-001 "
	if remoteOutputCorrect(redis, "done", canonical, canonical) {
		t.Fatal("plain done would incorrectly satisfy the redis-down degraded outcome")
	}
	if !remoteOutputCorrect(redis, "done_degraded", canonical, canonical) {
		t.Fatal("done_degraded did not satisfy the redis-down outcome")
	}
	for _, scenario := range []Scenario{ScenarioPodKill, ScenarioClientDrop, ScenarioSlowConsumer} {
		definition, found := definitionFor(scenario)
		if !found {
			t.Fatalf("%s definition is missing", scenario)
		}
		if definition.ExpectedOutcome != "done" {
			t.Fatalf("%s expected outcome = %q, want done", scenario, definition.ExpectedOutcome)
		}
		if remoteOutputCorrect(definition, "done_degraded", canonical, canonical) {
			t.Fatalf("%s accepted an unexpected degraded terminal", scenario)
		}
	}
}
