package conformance

import "testing"

func TestParseVerdict(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]Verdict{
		"safe": VerdictSafe, "DEGRADED": VerdictDegraded,
		"unsafe": VerdictUnsafe, "unknown": VerdictUnknown,
	} {
		got, err := ParseVerdict(input)
		if err != nil || got != want {
			t.Errorf("ParseVerdict(%q) = (%q, %v), want (%q, nil)", input, got, err, want)
		}
	}
	if _, err := ParseVerdict("maybe"); err == nil {
		t.Fatal("ParseVerdict accepted an undefined verdict")
	}
}

func TestTemplateModeValidate(t *testing.T) {
	t.Parallel()
	if err := TemplateStrict.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := TemplatePermissive.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := TemplateMode("unsafe").Validate(); err == nil {
		t.Fatal("undefined template mode accepted")
	}
}
