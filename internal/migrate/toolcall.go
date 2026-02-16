package migrate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalidToolCallChunk indicates a malformed OpenAI chat chunk supplied to
// the tool-call boundary tracker.
var ErrInvalidToolCallChunk = errors.New("migrate: invalid tool-call chunk")

// ToolCallTracker incrementally tracks fragmented tool calls by choice and
// tool index. Its zero value is ready for use by one producer goroutine.
type ToolCallTracker struct {
	choices map[int]*toolChoiceProgress
}

type toolChoiceProgress struct {
	calls            map[int]*toolCallProgress
	boundaryDeclared bool
}

type toolCallProgress struct {
	name      []byte
	arguments []byte
}

type parsedToolChoice struct {
	index            int
	calls            []parsedToolCall
	boundaryDeclared bool
}

type parsedToolCall struct {
	index     int
	name      string
	arguments string
}

// ObserveChunk applies one complete OpenAI chat-completion chunk. Malformed
// input changes no tracker state.
func (tracker *ToolCallTracker) ObserveChunk(payload []byte) error {
	parsed, err := parseToolCallChunk(payload)
	if err != nil {
		return err
	}
	if tracker.choices == nil {
		tracker.choices = make(map[int]*toolChoiceProgress)
	}
	for _, choice := range parsed {
		progress, exists := tracker.choices[choice.index]
		if !exists && len(choice.calls) != 0 {
			progress = &toolChoiceProgress{calls: make(map[int]*toolCallProgress)}
			tracker.choices[choice.index] = progress
		}
		if progress == nil {
			continue
		}
		for _, call := range choice.calls {
			callProgress, exists := progress.calls[call.index]
			if !exists {
				callProgress = &toolCallProgress{}
				progress.calls[call.index] = callProgress
			}
			callProgress.name = append(callProgress.name, call.name...)
			callProgress.arguments = append(callProgress.arguments, call.arguments...)
		}
		if choice.boundaryDeclared {
			progress.boundaryDeclared = true
		}
		if toolChoiceComplete(progress) {
			delete(tracker.choices, choice.index)
		}
	}
	return nil
}

// InsideToolCall reports whether migration would land after a tool-call delta
// but before complete arguments and a declared tool-call boundary.
func (tracker *ToolCallTracker) InsideToolCall() bool {
	return tracker != nil && len(tracker.choices) != 0
}

// ActiveToolCalls returns the number of independently tracked tool indices.
func (tracker *ToolCallTracker) ActiveToolCalls() int {
	if tracker == nil {
		return 0
	}
	count := 0
	for _, choice := range tracker.choices {
		count += len(choice.calls)
	}
	return count
}

func parseToolCallChunk(payload []byte) ([]parsedToolChoice, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !json.Valid(trimmed) {
		return nil, fmt.Errorf("%w: payload is not a valid JSON object", ErrInvalidToolCallChunk)
	}
	var chunk struct {
		Choices []struct {
			Index        *int            `json:"index"`
			Delta        json.RawMessage `json:"delta"`
			FinishReason *string         `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(trimmed, &chunk); err != nil {
		return nil, fmt.Errorf("%w: decode choices: %w", ErrInvalidToolCallChunk, err)
	}
	parsed := make([]parsedToolChoice, 0, len(chunk.Choices))
	for _, choice := range chunk.Choices {
		if choice.Index == nil || *choice.Index < 0 {
			return nil, fmt.Errorf("%w: each choice requires a non-negative index", ErrInvalidToolCallChunk)
		}
		parsedChoice := parsedToolChoice{
			index:            *choice.Index,
			boundaryDeclared: choice.FinishReason != nil && *choice.FinishReason == "tool_calls",
		}
		if len(choice.Delta) != 0 && !isJSONNull(choice.Delta) {
			var delta struct {
				ToolCalls json.RawMessage `json:"tool_calls"`
			}
			if err := json.Unmarshal(choice.Delta, &delta); err != nil {
				return nil, fmt.Errorf("%w: choice %d delta: %w", ErrInvalidToolCallChunk, *choice.Index, err)
			}
			if len(delta.ToolCalls) != 0 && !isJSONNull(delta.ToolCalls) {
				var calls []struct {
					Index    *int `json:"index"`
					Function *struct {
						Name      *string `json:"name"`
						Arguments *string `json:"arguments"`
					} `json:"function"`
				}
				if err := json.Unmarshal(delta.ToolCalls, &calls); err != nil || calls == nil {
					if err != nil {
						return nil, fmt.Errorf("%w: choice %d tool_calls: %w", ErrInvalidToolCallChunk, *choice.Index, err)
					}
					return nil, fmt.Errorf("%w: choice %d tool_calls must be an array", ErrInvalidToolCallChunk, *choice.Index)
				}
				for _, call := range calls {
					if call.Index == nil || *call.Index < 0 {
						return nil, fmt.Errorf("%w: tool call requires a non-negative index", ErrInvalidToolCallChunk)
					}
					parsedCall := parsedToolCall{index: *call.Index}
					if call.Function != nil {
						if call.Function.Name != nil {
							parsedCall.name = *call.Function.Name
						}
						if call.Function.Arguments != nil {
							parsedCall.arguments = *call.Function.Arguments
						}
					}
					parsedChoice.calls = append(parsedChoice.calls, parsedCall)
				}
			}
		}
		parsed = append(parsed, parsedChoice)
	}
	return parsed, nil
}

func toolChoiceComplete(choice *toolChoiceProgress) bool {
	if !choice.boundaryDeclared || len(choice.calls) == 0 {
		return false
	}
	for _, call := range choice.calls {
		if len(bytes.TrimSpace(call.arguments)) == 0 || !json.Valid(call.arguments) {
			return false
		}
	}
	return true
}
