package proxy

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestNormalizeRequestPreservesUnknownFields(t *testing.T) {
	t.Parallel()

	input := []byte(`{
  "vendor_extension": {"threshold": 0.125, "tokens": [1, 2, 3]},
  "messages": [{"role":"user","content":"hello"}],
  "stream": true,
  "model": "model-a"
}`)
	original := append([]byte(nil), input...)
	got, err := normalizeRequest(input)
	if err != nil {
		t.Fatalf("normalizeRequest() error = %v", err)
	}
	if got.Model != "model-a" || !got.Stream {
		t.Fatalf("normalizeRequest() metadata = model %q stream %v", got.Model, got.Stream)
	}
	if !reflect.DeepEqual(input, original) {
		t.Fatal("normalizeRequest() mutated its input")
	}

	var before, after any
	if err := json.Unmarshal(input, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got.Body, &after); err != nil {
		t.Fatalf("canonical body is invalid JSON: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("canonical body changed request semantics:\n got %#v\nwant %#v", after, before)
	}
}

func TestNormalizeRequestIsCanonical(t *testing.T) {
	t.Parallel()

	first, err := normalizeRequest([]byte(`{"z":3,"stream":false,"a":{"b":2,"a":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := normalizeRequest([]byte(` { "a" : { "b" : 2, "a" : 1 }, "z" : 3, "stream" : false } `))
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Body) != string(second.Body) {
		t.Fatalf("equivalent requests normalized differently:\n%s\n%s", first.Body, second.Body)
	}
	if first.Stream || second.Stream {
		t.Fatal("false stream field was not retained as false metadata")
	}
}

func TestNormalizeRequestDefaultsOptionalMetadata(t *testing.T) {
	t.Parallel()

	got, err := normalizeRequest([]byte(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.Stream || got.Model != "" {
		t.Fatalf("defaults = stream %v model %q", got.Stream, got.Model)
	}
}

func TestNormalizeRequestRejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		objectKind bool
	}{
		{name: "empty", body: ``},
		{name: "malformed", body: `{"stream":`},
		{name: "null", body: `null`, objectKind: true},
		{name: "array", body: `[]`, objectKind: true},
		{name: "trailing value", body: `{} {}`},
		{name: "stream string", body: `{"stream":"true"}`},
		{name: "stream null", body: `{"stream":null}`},
		{name: "model number", body: `{"model":42}`},
		{name: "model null", body: `{"model":null}`},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeRequest([]byte(test.body))
			if err == nil {
				t.Fatal("normalizeRequest() returned nil error")
			}
			if test.objectKind && !errors.Is(err, errRequestNotJSONObject) {
				t.Fatalf("normalizeRequest() error = %v, want errRequestNotJSONObject", err)
			}
		})
	}
}
