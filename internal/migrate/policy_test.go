package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/conformance"
)

func TestDefaultPolicyAndValidation(t *testing.T) {
	t.Parallel()

	want := Policy{
		MaxMigrations:      3,
		MaxMigrationTokens: 8192,
		MaxStreamDuration:  15 * time.Minute,
		SeamWindowBytes:    64,
		TemplateMode:       conformance.TemplateStrict,
	}
	policy := DefaultPolicy()
	if !reflect.DeepEqual(policy, want) {
		t.Fatalf("DefaultPolicy() = %#v, want %#v", policy, want)
	}
	if err := policy.Validate(); err != nil {
		t.Fatalf("DefaultPolicy().Validate() error = %v", err)
	}

	zeroLimits := policy
	zeroLimits.MaxMigrations = 0
	zeroLimits.MaxMigrationTokens = 0
	if err := zeroLimits.Validate(); err != nil {
		t.Fatalf("zero disabling limits Validate() error = %v", err)
	}
}

func TestPolicyValidateReportsEveryProblem(t *testing.T) {
	t.Parallel()

	policy := DefaultPolicy()
	policy.MaxStreamDuration = 0
	policy.SeamWindowBytes = -1
	policy.TemplateMode = conformance.TemplateMode("unknown")
	err := policy.Validate()
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("Validate() error = %v, want ErrInvalidPolicy", err)
	}
	for _, fragment := range []string{
		"maximum stream duration",
		"seam window bytes",
		"template mode",
	} {
		if !containsErrorText(err, fragment) {
			t.Errorf("Validate() error %q does not contain %q", err, fragment)
		}
	}
}

func containsErrorText(err error, fragment string) bool {
	return err != nil && strings.Contains(err.Error(), fragment)
}
