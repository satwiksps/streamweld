package migrate

import (
	"reflect"
	"testing"
)

func TestEvaluateCorrectnessReturnsEveryFailure(t *testing.T) {
	t.Parallel()

	result := EvaluateCorrectness(CorrectnessSnapshot{
		ToolCallInProgress: true,
		StructuredResponse: true,
		AccumulatedText:    []byte(`{"invalid":]`),
		MultipleChoices:    true,
	})
	want := []CorrectnessFailure{
		FailureToolCallBoundary,
		FailureStructuredPrefixInvalid,
		FailureUnsupportedContinuationShape,
	}
	if !reflect.DeepEqual(result.Failures, want) {
		t.Fatalf("failures = %#v, want %#v", result.Failures, want)
	}
	if result.Eligible() {
		t.Fatal("failed correctness result reported eligible")
	}
}

func TestEvaluateCorrectnessStructuredPrefixBoundaries(t *testing.T) {
	t.Parallel()

	for _, prefix := range [][]byte{nil, []byte(`{"answer":`), []byte(`{"answer":[1,2]}`)} {
		result := EvaluateCorrectness(CorrectnessSnapshot{StructuredResponse: true, AccumulatedText: prefix})
		if !result.Eligible() {
			t.Errorf("structured prefix %q refused: %#v", prefix, result)
		}
	}
	invalid := EvaluateCorrectness(CorrectnessSnapshot{StructuredResponse: true, AccumulatedText: []byte(`{"answer":]`)})
	if !reflect.DeepEqual(invalid.Failures, []CorrectnessFailure{FailureStructuredPrefixInvalid}) {
		t.Fatalf("invalid structured prefix failures = %#v", invalid.Failures)
	}
	ignored := EvaluateCorrectness(CorrectnessSnapshot{AccumulatedText: []byte(`{"answer":]`)})
	if !ignored.Eligible() {
		t.Fatalf("text response incorrectly parsed as structured: %#v", ignored)
	}
}

func TestEvaluateCorrectnessIndividualRefusals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot CorrectnessSnapshot
		failure  CorrectnessFailure
	}{
		{name: "tool call", snapshot: CorrectnessSnapshot{ToolCallInProgress: true}, failure: FailureToolCallBoundary},
		{name: "multiple choices", snapshot: CorrectnessSnapshot{MultipleChoices: true}, failure: FailureUnsupportedContinuationShape},
		{name: "structured prefix", snapshot: CorrectnessSnapshot{StructuredResponse: true, AccumulatedText: []byte(`+`)}, failure: FailureStructuredPrefixInvalid},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := EvaluateCorrectness(test.snapshot)
			if !reflect.DeepEqual(result.Failures, []CorrectnessFailure{test.failure}) {
				t.Fatalf("failures = %#v, want [%q]", result.Failures, test.failure)
			}
		})
	}
}
