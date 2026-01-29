package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type tokenUsage struct {
	PromptTokens     uint64 `json:"prompt_tokens"`
	CompletionTokens uint64 `json:"completion_tokens"`
	TotalTokens      uint64 `json:"total_tokens"`
	Estimated        bool   `json:"estimated"`
}

type chunkObservation struct {
	TextDelta    string
	Usage        *tokenUsage
	FinishReason string
	ErrorPayload json.RawMessage
	ToolCall     bool
}

type openAIChunk struct {
	Choices []struct {
		Index        int             `json:"index"`
		Text         *string         `json:"text"`
		FinishReason *string         `json:"finish_reason"`
		Delta        json.RawMessage `json:"delta"`
	} `json:"choices"`
	Usage *tokenUsage     `json:"usage"`
	Error json.RawMessage `json:"error"`
}

type chatDelta struct {
	Content      *string         `json:"content"`
	ToolCalls    json.RawMessage `json:"tool_calls"`
	FunctionCall json.RawMessage `json:"function_call"`
}

func observeOpenAIChunk(data []byte) (chunkObservation, error) {
	var chunk openAIChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return chunkObservation{}, fmt.Errorf("decode upstream SSE data as OpenAI chunk: %w", err)
	}
	observation := chunkObservation{Usage: chunk.Usage}
	if len(bytes.TrimSpace(chunk.Error)) > 0 && !bytes.Equal(bytes.TrimSpace(chunk.Error), []byte("null")) {
		observation.ErrorPayload = append(json.RawMessage(nil), chunk.Error...)
	}

	for _, choice := range chunk.Choices {
		if choice.Index != 0 {
			continue
		}
		if choice.Text != nil {
			observation.TextDelta += *choice.Text
		}
		if choice.FinishReason != nil {
			observation.FinishReason = *choice.FinishReason
		}
		if len(choice.Delta) == 0 || bytes.Equal(bytes.TrimSpace(choice.Delta), []byte("null")) {
			continue
		}
		var delta chatDelta
		if err := json.Unmarshal(choice.Delta, &delta); err != nil {
			return chunkObservation{}, fmt.Errorf("decode chat completion delta: %w", err)
		}
		if delta.Content != nil {
			observation.TextDelta += *delta.Content
		}
		if isPresentJSON(delta.ToolCalls) || isPresentJSON(delta.FunctionCall) {
			observation.ToolCall = true
		}
	}
	return observation, nil
}

func isPresentJSON(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) && !bytes.Equal(trimmed, []byte("[]"))
}

type streamProgress struct {
	text       []byte
	usage      tokenUsage
	exactUsage bool
	toolCall   bool
}

func (p *streamProgress) Apply(observation chunkObservation) {
	p.text = append(p.text, observation.TextDelta...)
	if observation.Usage != nil {
		p.usage = *observation.Usage
		p.usage.Estimated = false
		p.exactUsage = true
	}
	if observation.ToolCall {
		p.toolCall = true
	}
}

func (p *streamProgress) Snapshot() (string, tokenUsage) {
	usage := p.usage
	if !p.exactUsage {
		usage.CompletionTokens = uint64(len(p.text))
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
		usage.Estimated = true
	}
	return string(append([]byte(nil), p.text...)), usage
}
