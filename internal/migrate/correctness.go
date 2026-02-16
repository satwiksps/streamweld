package migrate

// CorrectnessFailure is a stable warning code for a request-shape gate that
// refuses migration after policy eligibility succeeds.
type CorrectnessFailure string

// Correctness failure codes identify the protocol gate that refused migration.
const (
	FailureToolCallBoundary             CorrectnessFailure = "tool_call_boundary"
	FailureStructuredPrefixInvalid      CorrectnessFailure = "structured_prefix_invalid"
	FailureUnsupportedContinuationShape CorrectnessFailure = "unsupported_continuation_shape"
)

// CorrectnessSnapshot captures the correctness state at one migration point.
type CorrectnessSnapshot struct {
	ToolCallInProgress bool
	StructuredResponse bool
	AccumulatedText    []byte
	MultipleChoices    bool
}

// CorrectnessResult contains every failed correctness gate in protocol order.
type CorrectnessResult struct {
	Failures []CorrectnessFailure
}

// Eligible reports whether all correctness gates passed.
func (result CorrectnessResult) Eligible() bool {
	return len(result.Failures) == 0
}

// EvaluateCorrectness evaluates every request-shape gate without
// short-circuiting. The structured prefix is checked only for a structured
// response; policy admission of structured resume is handled by eligibility.
func EvaluateCorrectness(snapshot CorrectnessSnapshot) CorrectnessResult {
	result := CorrectnessResult{Failures: make([]CorrectnessFailure, 0, 3)}
	if snapshot.ToolCallInProgress {
		result.Failures = append(result.Failures, FailureToolCallBoundary)
	}
	if snapshot.StructuredResponse && ValidateJSONPrefix(snapshot.AccumulatedText) != nil {
		result.Failures = append(result.Failures, FailureStructuredPrefixInvalid)
	}
	if snapshot.MultipleChoices {
		result.Failures = append(result.Failures, FailureUnsupportedContinuationShape)
	}
	return result
}
