package migrate

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"unicode/utf8"
)

func TestRewriteChatContinuationPreservesFieldsAndAppendsAssistant(t *testing.T) {
	t.Parallel()

	original := []byte(`{
		"model":"model-a",
		"messages":[{"role":"system","content":"rules"},{"role":"user","content":"question"}],
		"stream":false,
		"temperature":0.25,
		"seed":9007199254740993,
		"max_tokens":8,
		"max_completion_tokens":10,
		"vendor":{"nested":[1,{"preserve":true}]}
	}`)
	rewritten, err := RewriteContinuation(RequestChatCompletion, original, ContinuationOptions{
		AccumulatedText:      "answer so far 雪",
		TokensAlreadyEmitted: 7,
	})
	if err != nil {
		t.Fatalf("RewriteContinuation() error = %v", err)
	}
	fields := decodeRewrittenRequest(t, rewritten)
	if string(fields["model"]) != `"model-a"` || string(fields["temperature"]) != "0.25" {
		t.Fatalf("sampling/model fields changed: %s", rewritten)
	}
	if string(fields["seed"]) != "9007199254740993" {
		t.Fatalf("seed = %s, want original exact integer", fields["seed"])
	}
	if string(fields["max_tokens"]) != "1" || string(fields["max_completion_tokens"]) != "3" {
		t.Fatalf("remaining token limits = max_tokens:%s max_completion_tokens:%s", fields["max_tokens"], fields["max_completion_tokens"])
	}
	if string(fields["stream"]) != "true" || string(fields["continue_final_message"]) != "true" || string(fields["add_generation_prompt"]) != "false" {
		t.Fatalf("continuation flags missing: %s", rewritten)
	}
	var vendor map[string]json.RawMessage
	if err := json.Unmarshal(fields["vendor"], &vendor); err != nil || len(vendor["nested"]) == 0 {
		t.Fatalf("unknown vendor field was not preserved: %s (%v)", fields["vendor"], err)
	}
	var messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(fields["messages"], &messages); err != nil {
		t.Fatalf("decode rewritten messages: %v", err)
	}
	if len(messages) != 3 || messages[2].Role != "assistant" || messages[2].Content != "answer so far 雪" {
		t.Fatalf("rewritten messages = %#v", messages)
	}
}

func TestRewriteLegacyContinuationPreservesEligibleShape(t *testing.T) {
	t.Parallel()

	original := []byte(`{"model":"legacy","prompt":"Once ","n":1,"stream":false,"max_tokens":9,"seed":17,"suffix":"!","vendor":true}`)
	rewritten, err := RewriteContinuation(RequestCompletion, original, ContinuationOptions{
		AccumulatedText:      "upon a time",
		TokensAlreadyEmitted: 4,
	})
	if err != nil {
		t.Fatalf("RewriteContinuation() error = %v", err)
	}
	fields := decodeRewrittenRequest(t, rewritten)
	if string(fields["prompt"]) != `"Once upon a time"` {
		t.Fatalf("legacy prompt = %s", fields["prompt"])
	}
	if string(fields["max_tokens"]) != "5" || string(fields["seed"]) != "17" || string(fields["stream"]) != "true" {
		t.Fatalf("legacy rewritten fields = %s", rewritten)
	}
	if _, ok := fields["continue_final_message"]; ok {
		t.Fatalf("legacy request received chat-only flag: %s", rewritten)
	}
	if _, ok := fields["add_generation_prompt"]; ok {
		t.Fatalf("legacy request received chat-only flag: %s", rewritten)
	}
	if string(fields["suffix"]) != `"!"` || string(fields["vendor"]) != "true" {
		t.Fatalf("legacy unknown fields changed: %s", rewritten)
	}
}

func TestRewriteContinuationTokenLimitBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   string
		emitted uint64
		want    string
		field   string
		wantErr bool
	}{
		{name: "one remaining", limit: "8", emitted: 7, want: "1", field: "max_tokens"},
		{name: "equal exhausted", limit: "8", emitted: 8, wantErr: true, field: "max_tokens"},
		{name: "emitted exceeds limit", limit: "8", emitted: 9, wantErr: true, field: "max_tokens"},
		{name: "zero original exhausted", limit: "0", emitted: 0, wantErr: true, field: "max_tokens"},
		{name: "completion limit", limit: "100", emitted: 37, want: "63", field: "max_completion_tokens"},
		{name: "completion limit exhausted", limit: "100", emitted: 100, wantErr: true, field: "max_completion_tokens"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			original := []byte(`{"messages":[],"` + test.field + `":` + test.limit + `}`)
			rewritten, err := RewriteContinuation(RequestChatCompletion, original, ContinuationOptions{TokensAlreadyEmitted: test.emitted})
			if test.wantErr {
				if !errors.Is(err, ErrTokenBudgetExhausted) || rewritten != nil {
					t.Fatalf("RewriteContinuation() = %s, %v; want no request and ErrTokenBudgetExhausted", rewritten, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RewriteContinuation() error = %v", err)
			}
			fields := decodeRewrittenRequest(t, rewritten)
			if got := string(fields[test.field]); got != test.want {
				t.Fatalf("%s = %s, want %s", test.field, got, test.want)
			}
		})
	}

	rewritten, err := RewriteContinuation(RequestChatCompletion, []byte(`{"messages":[],"max_tokens":null,"seed":null}`), ContinuationOptions{TokensAlreadyEmitted: 99})
	if err != nil {
		t.Fatalf("RewriteContinuation(null limit) error = %v", err)
	}
	fields := decodeRewrittenRequest(t, rewritten)
	if string(fields["max_tokens"]) != "null" || string(fields["seed"]) != "null" {
		t.Fatalf("null optional fields changed: %s", rewritten)
	}
	if _, ok := fields["max_completion_tokens"]; ok {
		t.Fatalf("absent max_completion_tokens was inserted: %s", rewritten)
	}
}

func TestRewriteContinuationRefusesUnsupportedShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kind     RequestKind
		original []byte
	}{
		{name: "chat multiple choices", kind: RequestChatCompletion, original: []byte(`{"messages":[],"n":2}`)},
		{name: "legacy multiple choices", kind: RequestCompletion, original: []byte(`{"prompt":"x","n":2}`)},
		{name: "legacy array prompt", kind: RequestCompletion, original: []byte(`{"prompt":["x","y"]}`)},
		{name: "legacy missing prompt", kind: RequestCompletion, original: []byte(`{}`)},
		{name: "legacy null prompt", kind: RequestCompletion, original: []byte(`{"prompt":null}`)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := RewriteContinuation(test.kind, test.original, ContinuationOptions{})
			if !errors.Is(err, ErrUnsupportedContinuationShape) {
				t.Fatalf("RewriteContinuation() error = %v, want ErrUnsupportedContinuationShape", err)
			}
		})
	}
}

func TestRewriteContinuationRejectsMalformedInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kind     RequestKind
		original []byte
		options  ContinuationOptions
	}{
		{name: "unknown request kind", kind: RequestKind("unknown"), original: []byte(`{}`)},
		{name: "invalid JSON", kind: RequestChatCompletion, original: []byte(`{"messages":`)},
		{name: "non-object", kind: RequestChatCompletion, original: []byte(`[]`)},
		{name: "invalid UTF-8 request", kind: RequestChatCompletion, original: []byte{'{', '"', 0xff, '"', ':', '1', '}'}},
		{name: "invalid UTF-8 accumulated", kind: RequestChatCompletion, original: []byte(`{"messages":[]}`), options: ContinuationOptions{AccumulatedText: string([]byte{0xff})}},
		{name: "missing messages", kind: RequestChatCompletion, original: []byte(`{}`)},
		{name: "null messages", kind: RequestChatCompletion, original: []byte(`{"messages":null}`)},
		{name: "object messages", kind: RequestChatCompletion, original: []byte(`{"messages":{}}`)},
		{name: "zero choices", kind: RequestChatCompletion, original: []byte(`{"messages":[],"n":0}`)},
		{name: "fractional choices", kind: RequestChatCompletion, original: []byte(`{"messages":[],"n":1.5}`)},
		{name: "string choices", kind: RequestChatCompletion, original: []byte(`{"messages":[],"n":"1"}`)},
		{name: "negative max tokens", kind: RequestChatCompletion, original: []byte(`{"messages":[],"max_tokens":-1}`)},
		{name: "fractional max tokens", kind: RequestChatCompletion, original: []byte(`{"messages":[],"max_tokens":1.5}`)},
		{name: "string max tokens", kind: RequestChatCompletion, original: []byte(`{"messages":[],"max_tokens":"5"}`)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := RewriteContinuation(test.kind, test.original, test.options)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("RewriteContinuation() error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestIsStructuredRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		request    string
		structured bool
		wantError  bool
	}{
		{name: "absent", request: `{}`},
		{name: "null", request: `{"response_format":null}`},
		{name: "text", request: `{"response_format":{"type":"text"}}`},
		{name: "json object", request: `{"response_format":{"type":"json_object"}}`, structured: true},
		{name: "json schema", request: `{"response_format":{"type":"json_schema","json_schema":{"name":"x"}}}`, structured: true},
		{name: "unknown extensible type", request: `{"response_format":{"type":"vendor"}}`},
		{name: "non-object response format", request: `{"response_format":"json_object"}`, wantError: true},
		{name: "non-string type", request: `{"response_format":{"type":1}}`, wantError: true},
		{name: "invalid request", request: `[]`, wantError: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			structured, err := IsStructuredRequest([]byte(test.request))
			if (err != nil) != test.wantError {
				t.Fatalf("IsStructuredRequest() error = %v, wantError %v", err, test.wantError)
			}
			if structured != test.structured {
				t.Fatalf("IsStructuredRequest() = %v, want %v", structured, test.structured)
			}
		})
	}
}

func FuzzRewriteChatContinuation(f *testing.F) {
	f.Add("prefix", uint64(0))
	f.Add("multibyte 雪", uint64(7))
	f.Add("exact limit", uint64(25))
	f.Add("line\nnext", uint64(100))
	f.Fuzz(func(t *testing.T, accumulated string, emitted uint64) {
		if !utf8.ValidString(accumulated) {
			return
		}
		original := []byte(`{"model":"m","messages":[],"max_tokens":25,"seed":123,"vendor":{"keep":true}}`)
		rewritten, err := RewriteContinuation(RequestChatCompletion, original, ContinuationOptions{
			AccumulatedText: accumulated, TokensAlreadyEmitted: emitted,
		})
		if emitted >= 25 {
			if !errors.Is(err, ErrTokenBudgetExhausted) || rewritten != nil {
				t.Fatalf("RewriteContinuation() = %s, %v; want no request and ErrTokenBudgetExhausted", rewritten, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("RewriteContinuation() error = %v", err)
		}
		if !json.Valid(rewritten) {
			t.Fatalf("rewritten request is invalid JSON: %q", rewritten)
		}
		fields := decodeRewrittenRequest(t, rewritten)
		if string(fields["seed"]) != "123" || !bytes.Contains(fields["vendor"], []byte(`"keep":true`)) {
			t.Fatalf("rewritten request lost retained fields: %s", rewritten)
		}
		var messages []map[string]string
		if err := json.Unmarshal(fields["messages"], &messages); err != nil || len(messages) != 1 || messages[0]["content"] != accumulated {
			t.Fatalf("rewritten assistant message = %#v, error = %v", messages, err)
		}
	})
}

func decodeRewrittenRequest(t *testing.T, request []byte) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(request, &fields); err != nil {
		t.Fatalf("decode rewritten request: %v", err)
	}
	return fields
}
