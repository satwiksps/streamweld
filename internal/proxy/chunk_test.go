package proxy

import (
	"encoding/json"
	"testing"
)

func TestObserveOpenAIChatChunk(t *testing.T) {
	t.Parallel()

	got, err := observeOpenAIChunk([]byte(`{
  "choices":[
    {"index":1,"delta":{"content":"ignored"},"finish_reason":null},
    {"index":0,"delta":{"content":"snowman ☃","tool_calls":[{"index":0,"function":{"arguments":"{\"q\":"}}]},"finish_reason":"tool_calls"}
  ],
  "usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}
}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.TextDelta != "snowman ☃" || got.FinishReason != "tool_calls" || !got.ToolCall {
		t.Fatalf("observation = %#v", got)
	}
	if got.Usage == nil || got.Usage.TotalTokens != 10 {
		t.Fatalf("usage = %#v", got.Usage)
	}
}

func TestObserveOpenAILegacyAndErrorChunks(t *testing.T) {
	t.Parallel()

	legacy, err := observeOpenAIChunk([]byte(`{"choices":[{"index":0,"text":"hello","finish_reason":null}]}`))
	if err != nil || legacy.TextDelta != "hello" {
		t.Fatalf("legacy observation = %#v, error = %v", legacy, err)
	}
	errorChunk, err := observeOpenAIChunk([]byte(`{"error":{"message":"backend failed","code":"oom"}}`))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(errorChunk.ErrorPayload, &payload); err != nil {
		t.Fatalf("error payload = %s: %v", errorChunk.ErrorPayload, err)
	}
	if payload["code"] != "oom" {
		t.Fatalf("error payload = %#v", payload)
	}
}

func TestObserveOpenAIChunkRejectsMalformedJSON(t *testing.T) {
	t.Parallel()

	if _, err := observeOpenAIChunk([]byte(`{"choices":`)); err == nil {
		t.Fatal("observeOpenAIChunk() returned nil error")
	}
	if _, err := observeOpenAIChunk([]byte(`{"choices":[{"index":0,"delta":42}]}`)); err == nil {
		t.Fatal("observeOpenAIChunk() accepted a non-object delta")
	}
}

func TestStreamProgressUsesExactOrConservativeUsage(t *testing.T) {
	t.Parallel()

	var progress streamProgress
	progress.Apply(chunkObservation{TextDelta: "é"})
	text, usage := progress.Snapshot()
	if text != "é" || usage.CompletionTokens != 2 || usage.TotalTokens != 2 || !usage.Estimated {
		t.Fatalf("estimated snapshot = (%q, %#v)", text, usage)
	}

	progress.Apply(chunkObservation{Usage: &tokenUsage{PromptTokens: 11, CompletionTokens: 4, TotalTokens: 15}})
	_, usage = progress.Snapshot()
	if usage.PromptTokens != 11 || usage.CompletionTokens != 4 || usage.TotalTokens != 15 || usage.Estimated {
		t.Fatalf("exact snapshot = %#v", usage)
	}
}
