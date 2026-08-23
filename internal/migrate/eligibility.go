package migrate

import (
	"fmt"
	"time"

	"github.com/satwiksps/streamweld/internal/conformance"
)

// Predicate is a stable machine-readable migration eligibility predicate.
type Predicate string

// Eligibility predicate codes are ordered as specified by the protocol.
const (
	PredicateMaxMigrations      Predicate = "max_migrations"
	PredicateMaxMigrationTokens Predicate = "max_migration_tokens"
	PredicateMaxStreamDuration  Predicate = "max_stream_duration"
	PredicateTemplateVerdict    Predicate = "template_verdict"
	PredicateStructuredResume   Predicate = "allow_structured_resume"
	PredicateModelVersion       Predicate = "model_version"
	PredicateBackendAvailable   Predicate = "backend_available"
)

// WarningCode is a stable warning emitted while admitting a continuation.
type WarningCode string

// Template warning codes describe permitted non-safe conformance verdicts.
const (
	WarningTemplateDegraded         WarningCode = "template_degraded"
	WarningTemplateUnsafePermissive WarningCode = "template_unsafe_permissive"
)

// EligibilitySnapshot is one immutable view of the stream and selected target
// used to evaluate all protocol predicates.
type EligibilitySnapshot struct {
	MigrationsUsed         uint64
	AccumulatedTokens      uint64
	Elapsed                time.Duration
	TemplateVerdict        conformance.Verdict
	StructuredResponse     bool
	OriginModelVersion     string
	TargetModelVersion     string
	TargetBackendAvailable bool
}

// EligibilityResult contains every failed predicate in protocol order and any
// template warning required for an otherwise admitted target.
type EligibilityResult struct {
	Failures []Predicate
	Warnings []WarningCode
}

// Eligible reports whether all policy predicates passed.
func (result EligibilityResult) Eligible() bool {
	return len(result.Failures) == 0
}

// EvaluateEligibility evaluates every predicate without short-circuiting.
func EvaluateEligibility(policy Policy, snapshot EligibilitySnapshot) (EligibilityResult, error) {
	if err := policy.Validate(); err != nil {
		return EligibilityResult{}, err
	}
	if snapshot.Elapsed < 0 {
		return EligibilityResult{}, fmt.Errorf("%w: elapsed duration cannot be negative", ErrInvalidSnapshot)
	}
	if !snapshot.TemplateVerdict.Valid() {
		return EligibilityResult{}, fmt.Errorf("%w: undefined template verdict %q", ErrInvalidSnapshot, snapshot.TemplateVerdict)
	}

	result := EligibilityResult{
		Failures: make([]Predicate, 0, 7),
		Warnings: make([]WarningCode, 0, 1),
	}
	if snapshot.MigrationsUsed >= policy.MaxMigrations {
		result.Failures = append(result.Failures, PredicateMaxMigrations)
	}
	if snapshot.AccumulatedTokens >= policy.MaxMigrationTokens {
		result.Failures = append(result.Failures, PredicateMaxMigrationTokens)
	}
	if snapshot.Elapsed >= policy.MaxStreamDuration {
		result.Failures = append(result.Failures, PredicateMaxStreamDuration)
	}

	templateAllowed, templateWarning := evaluateTemplate(policy.TemplateMode, snapshot.TemplateVerdict)
	if !templateAllowed {
		result.Failures = append(result.Failures, PredicateTemplateVerdict)
	}
	if templateWarning != "" {
		result.Warnings = append(result.Warnings, templateWarning)
	}
	if snapshot.StructuredResponse && !policy.AllowStructuredResume {
		result.Failures = append(result.Failures, PredicateStructuredResume)
	}
	versionsMatch := snapshot.OriginModelVersion != "" &&
		snapshot.TargetModelVersion != "" &&
		snapshot.OriginModelVersion == snapshot.TargetModelVersion
	if !versionsMatch && !policy.AllowCrossVersion {
		result.Failures = append(result.Failures, PredicateModelVersion)
	}
	if !snapshot.TargetBackendAvailable {
		result.Failures = append(result.Failures, PredicateBackendAvailable)
	}
	return result, nil
}

func evaluateTemplate(mode conformance.TemplateMode, verdict conformance.Verdict) (bool, WarningCode) {
	switch verdict {
	case conformance.VerdictSafe:
		return true, ""
	case conformance.VerdictDegraded:
		return true, WarningTemplateDegraded
	case conformance.VerdictUnsafe:
		if mode == conformance.TemplatePermissive {
			return true, WarningTemplateUnsafePermissive
		}
		return false, ""
	default:
		return false, ""
	}
}
