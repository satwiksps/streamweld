package migrate

import (
	"errors"
	"fmt"
	"time"

	"github.com/streamweld/streamweld/internal/conformance"
)

const (
	// DefaultMaxMigrations is the number of dispatched continuation attempts
	// allowed for one stream.
	DefaultMaxMigrations uint64 = 3
	// DefaultMaxMigrationTokens is the accumulated-token eligibility ceiling.
	DefaultMaxMigrationTokens uint64 = 8192
	// DefaultMaxStreamDuration is the elapsed-time eligibility ceiling.
	DefaultMaxStreamDuration = 15 * time.Minute
	// DefaultSeamWindowBytes bounds leading continuation overlap inspection.
	DefaultSeamWindowBytes = 64
)

var (
	// ErrInvalidPolicy indicates an invalid migration policy value.
	ErrInvalidPolicy = errors.New("migrate: invalid policy")
	// ErrInvalidSnapshot indicates inconsistent eligibility input.
	ErrInvalidSnapshot = errors.New("migrate: invalid eligibility snapshot")
)

// Policy contains the pure producer-migration policy fields from the protocol.
// A zero migration or token limit is valid and disables migration through the
// corresponding eligibility predicate.
type Policy struct {
	MaxMigrations         uint64
	MaxMigrationTokens    uint64
	MaxStreamDuration     time.Duration
	AllowStructuredResume bool
	AllowCrossVersion     bool
	SeamWindowBytes       int
	TemplateMode          conformance.TemplateMode
}

// DefaultPolicy returns the protocol defaults.
func DefaultPolicy() Policy {
	return Policy{
		MaxMigrations:      DefaultMaxMigrations,
		MaxMigrationTokens: DefaultMaxMigrationTokens,
		MaxStreamDuration:  DefaultMaxStreamDuration,
		SeamWindowBytes:    DefaultSeamWindowBytes,
		TemplateMode:       conformance.TemplateStrict,
	}
}

// Validate rejects values that cannot be evaluated safely. Zero count limits
// intentionally remain valid as a way to make migration ineligible.
func (policy Policy) Validate() error {
	var problems []error
	if policy.MaxStreamDuration <= 0 {
		problems = append(problems, errors.New("maximum stream duration must be positive"))
	}
	if policy.SeamWindowBytes <= 0 {
		problems = append(problems, errors.New("seam window bytes must be positive"))
	}
	if err := policy.TemplateMode.Validate(); err != nil {
		problems = append(problems, err)
	}
	if len(problems) != 0 {
		return fmt.Errorf("%w: %w", ErrInvalidPolicy, errors.Join(problems...))
	}
	return nil
}
