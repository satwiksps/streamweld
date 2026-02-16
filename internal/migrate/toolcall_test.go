package migrate

import (
	"errors"
	"testing"
)

func TestToolCallTrackerRequiresCompleteArgumentsAndDeclaredBoundary(t *testing.T) {
	t.Parallel()

	var tracker ToolCallTracker
	chunks := []struct {
		name        string
		payload     string
		inside      bool
		activeCalls int
	}{
		{
			name:    "ordinary content",
			payload: `{"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`,
		},
		{
			name:    "first fragmented call",
			payload: `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"get_","arguments":"{\"city\":"}}]},"finish_reason":null}]}`,
			inside:  true, activeCalls: 1,
		},
		{
			name:    "arguments become complete without boundary",
			payload: `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"weather","arguments":"\"Paris\"}"}}]},"finish_reason":null}]}`,
			inside:  true, activeCalls: 1,
		},
		{
			name:    "boundary declaration",
			payload: `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		},
	}
	for _, chunk := range chunks {
		if err := tracker.ObserveChunk([]byte(chunk.payload)); err != nil {
			t.Fatalf("ObserveChunk(%s) error = %v", chunk.name, err)
		}
		if got := tracker.InsideToolCall(); got != chunk.inside {
			t.Fatalf("after %s InsideToolCall() = %v, want %v", chunk.name, got, chunk.inside)
		}
		if got := tracker.ActiveToolCalls(); got != chunk.activeCalls {
			t.Fatalf("after %s ActiveToolCalls() = %d, want %d", chunk.name, got, chunk.activeCalls)
		}
	}
}

func TestToolCallTrackerHandlesMultipleChoicesAndToolIndices(t *testing.T) {
	t.Parallel()

	var tracker ToolCallTracker
	first := []byte(`{
		"choices":[
			{"index":0,"delta":{"tool_calls":[
				{"index":0,"function":{"name":"a","arguments":"{}"}},
				{"index":1,"function":{"name":"b","arguments":"["}}
			]},"finish_reason":null},
			{"index":1,"delta":{"tool_calls":[
				{"index":0,"function":{"name":"c","arguments":"true"}}
			]},"finish_reason":"tool_calls"}
		]
	}`)
	if err := tracker.ObserveChunk(first); err != nil {
		t.Fatalf("ObserveChunk(first) error = %v", err)
	}
	if !tracker.InsideToolCall() || tracker.ActiveToolCalls() != 2 {
		t.Fatalf("tracker after first = inside %v, active %d", tracker.InsideToolCall(), tracker.ActiveToolCalls())
	}
	second := []byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"]"}}]},"finish_reason":"tool_calls"}]}`)
	if err := tracker.ObserveChunk(second); err != nil {
		t.Fatalf("ObserveChunk(second) error = %v", err)
	}
	if tracker.InsideToolCall() || tracker.ActiveToolCalls() != 0 {
		t.Fatalf("tracker remained inside completed calls: active %d", tracker.ActiveToolCalls())
	}
}

func TestToolCallTrackerBoundaryBeforeCompleteArgumentsRemainsUnsafe(t *testing.T) {
	t.Parallel()

	var tracker ToolCallTracker
	if err := tracker.ObserveChunk([]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"x\":"}}]},"finish_reason":"tool_calls"}]}`)); err != nil {
		t.Fatalf("ObserveChunk() error = %v", err)
	}
	if !tracker.InsideToolCall() {
		t.Fatal("incomplete argument became safe merely because boundary was declared")
	}
	if err := tracker.ObserveChunk([]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":null}]}`)); err != nil {
		t.Fatalf("ObserveChunk(completion) error = %v", err)
	}
	if tracker.InsideToolCall() {
		t.Fatal("complete argument plus previously declared boundary remained unsafe")
	}
}

func TestToolCallTrackerMalformedChunkDoesNotChangeState(t *testing.T) {
	t.Parallel()

	var tracker ToolCallTracker
	valid := []byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"["}}]},"finish_reason":null}]}`)
	if err := tracker.ObserveChunk(valid); err != nil {
		t.Fatalf("ObserveChunk(valid) error = %v", err)
	}
	tests := [][]byte{
		[]byte(`not-json`),
		[]byte(`null`),
		[]byte(`{"choices":[{"delta":{}}]}`),
		[]byte(`{"choices":[{"index":-1,"delta":{}}]}`),
		[]byte(`{"choices":[{"index":0,"delta":{"tool_calls":{}}}]}`),
		[]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"function":{}}]}}]}`),
		[]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":-1}]}}]}`),
	}
	for _, payload := range tests {
		err := tracker.ObserveChunk(payload)
		if !errors.Is(err, ErrInvalidToolCallChunk) {
			t.Errorf("ObserveChunk(%q) error = %v, want ErrInvalidToolCallChunk", payload, err)
		}
		if !tracker.InsideToolCall() || tracker.ActiveToolCalls() != 1 {
			t.Fatalf("malformed chunk %q changed tracker state", payload)
		}
	}
}

func TestToolCallTrackerCompleteCallInOneChunkIsAtBoundary(t *testing.T) {
	t.Parallel()

	var tracker ToolCallTracker
	payload := []byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"lookup","arguments":"{\"id\":1}"}}]},"finish_reason":"tool_calls"}]}`)
	if err := tracker.ObserveChunk(payload); err != nil {
		t.Fatalf("ObserveChunk() error = %v", err)
	}
	if tracker.InsideToolCall() || tracker.ActiveToolCalls() != 0 {
		t.Fatalf("complete one-chunk tool call remained active")
	}
}

func FuzzToolCallTracker(f *testing.F) {
	f.Add([]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`))
	f.Add([]byte(`{"choices":[{"index":0,"delta":{"content":"text"}}]}`))
	f.Fuzz(func(_ *testing.T, payload []byte) {
		var tracker ToolCallTracker
		_ = tracker.ObserveChunk(payload)
		_ = tracker.InsideToolCall()
		_ = tracker.ActiveToolCalls()
	})
}
