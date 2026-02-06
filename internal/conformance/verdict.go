package conformance

import (
	"errors"
	"fmt"
	"strings"
)

// Verdict is the result of probing a backend's chat-template continuation
// behavior. The uppercase values are also used in Kubernetes status.
type Verdict string

const (
	// VerdictUnknown means no applicable successful probe is cached.
	VerdictUnknown Verdict = "UNKNOWN"
	// VerdictSafe means all continuation probes passed.
	VerdictSafe Verdict = "SAFE"
	// VerdictDegraded means core continuation works but a secondary probe failed.
	VerdictDegraded Verdict = "DEGRADED"
	// VerdictUnsafe means continuation may restart, stop, or corrupt output.
	VerdictUnsafe Verdict = "UNSAFE"
)

// ParseVerdict parses an uppercase protocol verdict.
func ParseVerdict(value string) (Verdict, error) {
	verdict := Verdict(strings.ToUpper(value))
	if !verdict.Valid() {
		return "", fmt.Errorf("invalid template verdict %q", value)
	}
	return verdict, nil
}

// Valid reports whether verdict is a defined protocol value.
func (verdict Verdict) Valid() bool {
	return verdict == VerdictUnknown || verdict == VerdictSafe || verdict == VerdictDegraded || verdict == VerdictUnsafe
}

// TemplateMode controls whether an unsafe template can be continued after a
// loud warning. Unknown templates are refused in every mode.
type TemplateMode string

const (
	// TemplateStrict refuses unsafe and unknown templates.
	TemplateStrict TemplateMode = "strict"
	// TemplatePermissive permits an unsafe template with a warning.
	TemplatePermissive TemplateMode = "permissive"
)

// Validate rejects an undefined template mode.
func (mode TemplateMode) Validate() error {
	if mode != TemplateStrict && mode != TemplatePermissive {
		return errors.New("template mode must be strict or permissive")
	}
	return nil
}
