package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// DefaultMaxMigrations is the protocol continuation-attempt limit.
	DefaultMaxMigrations int32 = 3
	// DefaultMaxMigrationTokens is the protocol accumulated-token limit.
	DefaultMaxMigrationTokens int64 = 8192
	// DefaultMaxStreamDuration is the protocol stream-age limit.
	DefaultMaxStreamDuration = 15 * time.Minute
	// DefaultOrphanPolicy is the safe resumable-disconnect behavior.
	DefaultOrphanPolicy = OrphanContinue
	// DefaultOrphanTimeout is the cancel_after reattachment grace period.
	DefaultOrphanTimeout = 60 * time.Second
	// DefaultSeamWindowBytes bounds overlap reconciliation.
	DefaultSeamWindowBytes int32 = 64
	// DefaultJournalTTL is the terminal journal retention period.
	DefaultJournalTTL = 10 * time.Minute
)

// DefaultDurabilityPolicySpec returns a fully materialized protocol-default spec.
func DefaultDurabilityPolicySpec() DurabilityPolicySpec {
	spec := DurabilityPolicySpec{}
	spec.ApplyDefaults()
	return spec
}

// ApplyDefaults materializes omitted Kubernetes-defaulted fields. Explicit
// zero migration limits remain zero and therefore disable migration.
func (policy *DurabilityPolicy) ApplyDefaults() {
	if policy == nil {
		return
	}
	policy.Spec.ApplyDefaults()
}

// ApplyDefaults materializes omitted fields without changing explicit values.
func (spec *DurabilityPolicySpec) ApplyDefaults() {
	if spec == nil {
		return
	}
	if spec.MaxMigrations == nil {
		spec.MaxMigrations = valuePointer(DefaultMaxMigrations)
	}
	if spec.MaxMigrationTokens == nil {
		spec.MaxMigrationTokens = valuePointer(DefaultMaxMigrationTokens)
	}
	if spec.MaxStreamDuration == nil {
		spec.MaxStreamDuration = &metav1.Duration{Duration: DefaultMaxStreamDuration}
	}
	if spec.OrphanPolicy == "" {
		spec.OrphanPolicy = DefaultOrphanPolicy
	}
	if spec.OrphanTimeout == nil {
		spec.OrphanTimeout = &metav1.Duration{Duration: DefaultOrphanTimeout}
	}
	if spec.SeamWindowBytes == nil {
		spec.SeamWindowBytes = valuePointer(DefaultSeamWindowBytes)
	}
	if spec.JournalTTL == nil {
		spec.JournalTTL = &metav1.Duration{Duration: DefaultJournalTTL}
	}
}

// WithDefaults returns an independent, fully materialized copy.
func (spec *DurabilityPolicySpec) WithDefaults() DurabilityPolicySpec {
	if spec == nil {
		return DefaultDurabilityPolicySpec()
	}
	clone := spec.DeepCopy()
	clone.ApplyDefaults()
	return *clone
}

func valuePointer[T any](value T) *T {
	return &value
}
