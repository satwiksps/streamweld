package migrate

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/conformance"
)

func TestEvaluateEligibilityPassesSafeSnapshot(t *testing.T) {
	t.Parallel()

	result, err := EvaluateEligibility(DefaultPolicy(), eligibleSnapshot())
	if err != nil {
		t.Fatalf("EvaluateEligibility() error = %v", err)
	}
	if !result.Eligible() || len(result.Failures) != 0 || len(result.Warnings) != 0 {
		t.Fatalf("EvaluateEligibility() = %#v, want eligible without warnings", result)
	}
}

func TestEvaluateEligibilityReturnsEveryFailedPredicateInProtocolOrder(t *testing.T) {
	t.Parallel()

	snapshot := EligibilitySnapshot{
		MigrationsUsed:         3,
		AccumulatedTokens:      8192,
		Elapsed:                15 * time.Minute,
		TemplateVerdict:        conformance.VerdictUnknown,
		StructuredResponse:     true,
		OriginModelVersion:     "",
		TargetModelVersion:     "sha256:target",
		TargetBackendAvailable: false,
	}
	result, err := EvaluateEligibility(DefaultPolicy(), snapshot)
	if err != nil {
		t.Fatalf("EvaluateEligibility() error = %v", err)
	}
	want := []Predicate{
		PredicateMaxMigrations,
		PredicateMaxMigrationTokens,
		PredicateMaxStreamDuration,
		PredicateTemplateVerdict,
		PredicateStructuredResume,
		PredicateModelVersion,
		PredicateBackendAvailable,
	}
	if !reflect.DeepEqual(result.Failures, want) {
		t.Fatalf("failures = %#v, want %#v", result.Failures, want)
	}
	if result.Eligible() {
		t.Fatal("result with failures reported eligible")
	}
}

func TestEvaluateEligibilityBoundaries(t *testing.T) {
	t.Parallel()

	policy := DefaultPolicy()
	tests := []struct {
		name      string
		mutate    func(*EligibilitySnapshot)
		predicate Predicate
	}{
		{name: "migrations equal limit", mutate: func(value *EligibilitySnapshot) { value.MigrationsUsed = policy.MaxMigrations }, predicate: PredicateMaxMigrations},
		{name: "tokens equal limit", mutate: func(value *EligibilitySnapshot) { value.AccumulatedTokens = policy.MaxMigrationTokens }, predicate: PredicateMaxMigrationTokens},
		{name: "elapsed equal limit", mutate: func(value *EligibilitySnapshot) { value.Elapsed = policy.MaxStreamDuration }, predicate: PredicateMaxStreamDuration},
		{name: "unknown template", mutate: func(value *EligibilitySnapshot) { value.TemplateVerdict = conformance.VerdictUnknown }, predicate: PredicateTemplateVerdict},
		{name: "structured disabled", mutate: func(value *EligibilitySnapshot) { value.StructuredResponse = true }, predicate: PredicateStructuredResume},
		{name: "origin version unknown", mutate: func(value *EligibilitySnapshot) { value.OriginModelVersion = "" }, predicate: PredicateModelVersion},
		{name: "target version unknown", mutate: func(value *EligibilitySnapshot) { value.TargetModelVersion = "" }, predicate: PredicateModelVersion},
		{name: "versions differ", mutate: func(value *EligibilitySnapshot) { value.TargetModelVersion = "sha256:other" }, predicate: PredicateModelVersion},
		{name: "no backend", mutate: func(value *EligibilitySnapshot) { value.TargetBackendAvailable = false }, predicate: PredicateBackendAvailable},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := eligibleSnapshot()
			test.mutate(&snapshot)
			result, err := EvaluateEligibility(policy, snapshot)
			if err != nil {
				t.Fatalf("EvaluateEligibility() error = %v", err)
			}
			if !reflect.DeepEqual(result.Failures, []Predicate{test.predicate}) {
				t.Fatalf("failures = %#v, want [%q]", result.Failures, test.predicate)
			}
		})
	}
}

func TestEvaluateEligibilityTemplateMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode        conformance.TemplateMode
		verdict     conformance.Verdict
		eligible    bool
		wantWarning WarningCode
	}{
		{mode: conformance.TemplateStrict, verdict: conformance.VerdictSafe, eligible: true},
		{mode: conformance.TemplatePermissive, verdict: conformance.VerdictSafe, eligible: true},
		{mode: conformance.TemplateStrict, verdict: conformance.VerdictDegraded, eligible: true, wantWarning: WarningTemplateDegraded},
		{mode: conformance.TemplatePermissive, verdict: conformance.VerdictDegraded, eligible: true, wantWarning: WarningTemplateDegraded},
		{mode: conformance.TemplateStrict, verdict: conformance.VerdictUnsafe},
		{mode: conformance.TemplatePermissive, verdict: conformance.VerdictUnsafe, eligible: true, wantWarning: WarningTemplateUnsafePermissive},
		{mode: conformance.TemplateStrict, verdict: conformance.VerdictUnknown},
		{mode: conformance.TemplatePermissive, verdict: conformance.VerdictUnknown},
	}
	for _, test := range tests {
		test := test
		t.Run(string(test.mode)+"/"+string(test.verdict), func(t *testing.T) {
			t.Parallel()
			policy := DefaultPolicy()
			policy.TemplateMode = test.mode
			snapshot := eligibleSnapshot()
			snapshot.TemplateVerdict = test.verdict
			result, err := EvaluateEligibility(policy, snapshot)
			if err != nil {
				t.Fatalf("EvaluateEligibility() error = %v", err)
			}
			if result.Eligible() != test.eligible {
				t.Fatalf("Eligible() = %v, want %v; result = %#v", result.Eligible(), test.eligible, result)
			}
			var wantWarnings []WarningCode
			if test.wantWarning != "" {
				wantWarnings = []WarningCode{test.wantWarning}
			}
			if !slices.Equal(result.Warnings, wantWarnings) {
				t.Fatalf("warnings = %#v, want %#v", result.Warnings, wantWarnings)
			}
		})
	}
}

func TestEvaluateEligibilityOverrides(t *testing.T) {
	t.Parallel()

	policy := DefaultPolicy()
	policy.AllowStructuredResume = true
	policy.AllowCrossVersion = true
	snapshot := eligibleSnapshot()
	snapshot.StructuredResponse = true
	snapshot.OriginModelVersion = ""
	snapshot.TargetModelVersion = "different"
	result, err := EvaluateEligibility(policy, snapshot)
	if err != nil || !result.Eligible() {
		t.Fatalf("EvaluateEligibility(overrides) = (%#v, %v), want eligible", result, err)
	}
}

func TestEvaluateEligibilityRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	policy := DefaultPolicy()
	policy.SeamWindowBytes = 0
	if _, err := EvaluateEligibility(policy, eligibleSnapshot()); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid policy error = %v, want ErrInvalidPolicy", err)
	}

	policy = DefaultPolicy()
	snapshot := eligibleSnapshot()
	snapshot.Elapsed = -time.Nanosecond
	if _, err := EvaluateEligibility(policy, snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("negative elapsed error = %v, want ErrInvalidSnapshot", err)
	}
	snapshot = eligibleSnapshot()
	snapshot.TemplateVerdict = conformance.Verdict("BROKEN")
	if _, err := EvaluateEligibility(policy, snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("invalid verdict error = %v, want ErrInvalidSnapshot", err)
	}
}

func eligibleSnapshot() EligibilitySnapshot {
	return EligibilitySnapshot{
		MigrationsUsed:         2,
		AccumulatedTokens:      8191,
		Elapsed:                15*time.Minute - time.Nanosecond,
		TemplateVerdict:        conformance.VerdictSafe,
		OriginModelVersion:     "sha256:same",
		TargetModelVersion:     "sha256:same",
		TargetBackendAvailable: true,
	}
}
