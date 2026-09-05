package migrate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

var (
	// ErrInvalidRequest indicates that normalized request JSON is malformed or
	// has a field type that cannot be rewritten safely.
	ErrInvalidRequest = errors.New("migrate: invalid continuation request")
	// ErrUnsupportedContinuationShape indicates a valid request shape that
	// cannot be represented by one assistant continuation in protocol v1.
	ErrUnsupportedContinuationShape = errors.New("migrate: unsupported continuation shape")
	// ErrTokenBudgetExhausted indicates that another generation request would
	// exceed an original request's supplied completion-token limit.
	ErrTokenBudgetExhausted = errors.New("migrate: completion token budget exhausted")
)

// RequestKind selects the OpenAI request shape to rewrite.
type RequestKind string

const (
	// RequestChatCompletion rewrites /v1/chat/completions messages.
	RequestChatCompletion RequestKind = "chat_completion"
	// RequestCompletion rewrites an eligible /v1/completions prompt.
	RequestCompletion RequestKind = "completion"
)

// ContinuationOptions contains immutable progress from the failed producer.
type ContinuationOptions struct {
	AccumulatedText      string
	TokensAlreadyEmitted uint64
}

// RewriteContinuation creates a streaming continuation request while
// retaining all unrecognized top-level fields. Explicit seed and sampling
// fields are left untouched. Every supplied token-limit field is recomputed
// independently. Exhausted limits return ErrTokenBudgetExhausted instead of
// authorizing another token.
func RewriteContinuation(kind RequestKind, original []byte, options ContinuationOptions) ([]byte, error) {
	if !utf8.Valid(original) || !utf8.ValidString(options.AccumulatedText) {
		return nil, fmt.Errorf("%w: request and accumulated text must be valid UTF-8", ErrInvalidRequest)
	}
	fields, err := decodeRequestObject(original)
	if err != nil {
		return nil, err
	}
	if err := requireSingleChoice(fields); err != nil {
		return nil, err
	}

	switch kind {
	case RequestChatCompletion:
		if err := rewriteChatMessages(fields, options.AccumulatedText); err != nil {
			return nil, err
		}
		fields["continue_final_message"] = json.RawMessage("true")
		fields["add_generation_prompt"] = json.RawMessage("false")
	case RequestCompletion:
		if err := rewriteLegacyPrompt(fields, options.AccumulatedText); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: unsupported request kind %q", ErrInvalidRequest, kind)
	}

	for _, name := range []string{"max_tokens", "max_completion_tokens"} {
		if err := recomputeTokenLimit(fields, name, options.TokensAlreadyEmitted); err != nil {
			return nil, err
		}
	}
	fields["stream"] = json.RawMessage("true")
	rewritten, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("%w: encode rewritten request: %w", ErrInvalidRequest, err)
	}
	return rewritten, nil
}

// IsStructuredRequest reports whether response_format.type requests a
// json_object or json_schema response.
func IsStructuredRequest(original []byte) (bool, error) {
	if !utf8.Valid(original) {
		return false, fmt.Errorf("%w: request must be valid UTF-8", ErrInvalidRequest)
	}
	fields, err := decodeRequestObject(original)
	if err != nil {
		return false, err
	}
	raw, ok := fields["response_format"]
	if !ok || isJSONNull(raw) {
		return false, nil
	}
	var responseFormat map[string]json.RawMessage
	if err := json.Unmarshal(raw, &responseFormat); err != nil || responseFormat == nil {
		if err != nil {
			return false, fmt.Errorf("%w: response_format must be an object: %w", ErrInvalidRequest, err)
		}
		return false, fmt.Errorf("%w: response_format must be an object", ErrInvalidRequest)
	}
	rawType, ok := responseFormat["type"]
	if !ok || isJSONNull(rawType) {
		return false, nil
	}
	var responseType string
	if err := json.Unmarshal(rawType, &responseType); err != nil {
		return false, fmt.Errorf("%w: response_format.type must be a string: %w", ErrInvalidRequest, err)
	}
	return responseType == "json_object" || responseType == "json_schema", nil
}

func decodeRequestObject(original []byte) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(original)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return nil, fmt.Errorf("%w: request must be a JSON object", ErrInvalidRequest)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return nil, fmt.Errorf("%w: decode request object: %w", ErrInvalidRequest, err)
	}
	if fields == nil {
		return nil, fmt.Errorf("%w: request must be a JSON object", ErrInvalidRequest)
	}
	return fields, nil
}

func requireSingleChoice(fields map[string]json.RawMessage) error {
	raw, ok := fields["n"]
	if !ok || isJSONNull(raw) {
		return nil
	}
	var choices uint64
	if err := json.Unmarshal(raw, &choices); err != nil {
		return fmt.Errorf("%w: n must be a positive integer: %w", ErrInvalidRequest, err)
	}
	if choices == 0 {
		return fmt.Errorf("%w: n must be positive", ErrInvalidRequest)
	}
	if choices != 1 {
		return fmt.Errorf("%w: n=%d cannot be represented by one continuation", ErrUnsupportedContinuationShape, choices)
	}
	return nil
}

func rewriteChatMessages(fields map[string]json.RawMessage, accumulated string) error {
	raw, ok := fields["messages"]
	if !ok || isJSONNull(raw) {
		return fmt.Errorf("%w: chat messages must be an array", ErrInvalidRequest)
	}
	var messages []json.RawMessage
	if err := json.Unmarshal(raw, &messages); err != nil || messages == nil {
		if err != nil {
			return fmt.Errorf("%w: chat messages must be an array: %w", ErrInvalidRequest, err)
		}
		return fmt.Errorf("%w: chat messages must be an array", ErrInvalidRequest)
	}
	assistant, err := json.Marshal(struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}{Role: "assistant", Content: accumulated})
	if err != nil {
		return fmt.Errorf("%w: encode assistant continuation: %w", ErrInvalidRequest, err)
	}
	messages = append(messages, assistant)
	encoded, err := json.Marshal(messages)
	if err != nil {
		return fmt.Errorf("%w: encode chat messages: %w", ErrInvalidRequest, err)
	}
	fields["messages"] = encoded
	return nil
}

func rewriteLegacyPrompt(fields map[string]json.RawMessage, accumulated string) error {
	raw, ok := fields["prompt"]
	if !ok || isJSONNull(raw) {
		return fmt.Errorf("%w: legacy completion requires one string prompt", ErrUnsupportedContinuationShape)
	}
	var prompt string
	if err := json.Unmarshal(raw, &prompt); err != nil {
		return fmt.Errorf("%w: legacy completion requires one string prompt: %w", ErrUnsupportedContinuationShape, err)
	}
	encoded, err := json.Marshal(prompt + accumulated)
	if err != nil {
		return fmt.Errorf("%w: encode legacy continuation prompt: %w", ErrInvalidRequest, err)
	}
	fields["prompt"] = encoded
	return nil
}

func recomputeTokenLimit(fields map[string]json.RawMessage, name string, emitted uint64) error {
	raw, ok := fields[name]
	if !ok || isJSONNull(raw) {
		return nil
	}
	var original uint64
	if err := json.Unmarshal(raw, &original); err != nil {
		return fmt.Errorf("%w: %s must be a non-negative integer: %w", ErrInvalidRequest, name, err)
	}
	if original <= emitted {
		return fmt.Errorf("%w: %s=%d with %d tokens already emitted", ErrTokenBudgetExhausted, name, original, emitted)
	}
	remaining := original - emitted
	fields[name] = json.RawMessage(strconv.FormatUint(remaining, 10))
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
