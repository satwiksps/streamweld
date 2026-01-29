package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var errRequestNotJSONObject = errors.New("request body must be a JSON object")

// normalizedRequest is the immutable request representation retained for a
// durable stream. Body is canonical JSON; unknown top-level fields remain raw
// JSON values so backend-specific extensions survive normalization.
type normalizedRequest struct {
	Body   []byte
	Model  string
	Stream bool
}

func normalizeRequest(body []byte) (normalizedRequest, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] != '{' {
		return normalizedRequest{}, errRequestNotJSONObject
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil {
		return normalizedRequest{}, fmt.Errorf("decode request JSON: %w", err)
	}
	if fields == nil {
		return normalizedRequest{}, errRequestNotJSONObject
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err == nil {
		return normalizedRequest{}, errors.New("request body contains more than one JSON value")
	} else if !errors.Is(err, io.EOF) {
		return normalizedRequest{}, fmt.Errorf("decode trailing request data: %w", err)
	}

	result := normalizedRequest{}
	if raw, ok := fields["stream"]; ok {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return normalizedRequest{}, errors.New("request field stream must be a boolean")
		}
		if err := json.Unmarshal(raw, &result.Stream); err != nil {
			return normalizedRequest{}, fmt.Errorf("request field stream must be a boolean: %w", err)
		}
	}
	if raw, ok := fields["model"]; ok {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return normalizedRequest{}, errors.New("request field model must be a string")
		}
		if err := json.Unmarshal(raw, &result.Model); err != nil {
			return normalizedRequest{}, fmt.Errorf("request field model must be a string: %w", err)
		}
	}

	canonical, err := json.Marshal(fields)
	if err != nil {
		return normalizedRequest{}, fmt.Errorf("canonicalize request JSON: %w", err)
	}
	result.Body = canonical
	return result, nil
}
