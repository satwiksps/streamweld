package migrate

import (
	"encoding/json"
	"errors"
	"testing"
	"unicode/utf8"
)

func TestValidateJSONPrefixAcceptsEveryExtendableBoundary(t *testing.T) {
	t.Parallel()

	valid := []string{
		"",
		" \t\r\n",
		"{",
		`{"`,
		`{"name`,
		`{"name"`,
		`{"name" `,
		`{"name":`,
		`{"name": [`,
		`{"name": [1`,
		`{"name": [1,`,
		`{"name": [1, {"snow":"雪"}]}`,
		`{"name": [1, {"snow":"雪"}]}` + " \n",
		`[`,
		`[true, false, nul`,
		`"unterminated`,
		`"escape\\`,
		`"escape\\u12`,
		`"complete"`,
		"\"multibyte 雪",
		"t",
		"tr",
		"tru",
		"true",
		"fals",
		"nul",
		"-",
		"-0",
		"123",
		"1.",
		"1.25",
		"1e",
		"1e+",
		"1e-9",
		`{"surrogate":"\ud800"}`,
	}
	for _, prefix := range valid {
		prefix := prefix
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()
			if err := ValidateJSONPrefix([]byte(prefix)); err != nil {
				t.Fatalf("ValidateJSONPrefix(%q) error = %v", prefix, err)
			}
			if !IsValidJSONPrefix([]byte(prefix)) {
				t.Fatalf("IsValidJSONPrefix(%q) = false", prefix)
			}
		})
	}
}

func TestValidateJSONPrefixRejectsNonExtendableInput(t *testing.T) {
	t.Parallel()

	invalid := [][]byte{
		[]byte(`+1`),
		[]byte(`.1`),
		[]byte(`01`),
		[]byte(`-x`),
		[]byte(`1.x`),
		[]byte(`1e]`),
		[]byte(`nux`),
		[]byte(`true false`),
		[]byte(`true,`),
		[]byte(`{]`),
		[]byte(`{"a",`),
		[]byte(`{"a":}`),
		[]byte(`{"a":1,}`),
		[]byte(`[}`),
		[]byte(`[1,]`),
		[]byte(`"bad\x`),
		[]byte(`"bad\u12x`),
		[]byte("\"raw\nnewline"),
		[]byte(`"closed"x`),
		{'"', 0xff},
		{'"', 0xc0, 0x80},
	}
	for _, prefix := range invalid {
		prefix := append([]byte(nil), prefix...)
		t.Run(string(prefix), func(t *testing.T) {
			t.Parallel()
			err := ValidateJSONPrefix(prefix)
			if !errors.Is(err, ErrInvalidJSONPrefix) {
				t.Fatalf("ValidateJSONPrefix(%q) error = %v, want ErrInvalidJSONPrefix", prefix, err)
			}
			if IsValidJSONPrefix(prefix) {
				t.Fatalf("IsValidJSONPrefix(%q) = true", prefix)
			}
		})
	}
}

func TestEveryPrefixOfValidJSONIsAccepted(t *testing.T) {
	t.Parallel()

	values := [][]byte{
		[]byte(`null`),
		[]byte(`-12.5e+7`),
		[]byte(`"text\\n\u2603 雪"`),
		[]byte(`{"a":[true,false,null,{"b":"value"}],"n":123}`),
	}
	for _, value := range values {
		if !json.Valid(value) {
			t.Fatalf("invalid test fixture %q", value)
		}
		for length := 0; length <= len(value); length++ {
			if err := ValidateJSONPrefix(value[:length]); err != nil {
				t.Fatalf("ValidateJSONPrefix(%q), prefix %d of %q: %v", value[:length], length, value, err)
			}
		}
	}
}

func FuzzValidateJSONPrefix(f *testing.F) {
	for _, seed := range []string{
		`{"choices":[{"delta":{"content":"hello"}}]}`,
		`[null,true,false,-1.2e3,"snow 雪"]`,
		`"escaped\\n\u2603"`,
		`{"nested":{"array":[1,2,3]}}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		_ = ValidateJSONPrefix(value)
		if !utf8.Valid(value) || !json.Valid(value) {
			return
		}
		for length := 0; length <= len(value); length++ {
			if err := ValidateJSONPrefix(value[:length]); err != nil {
				t.Fatalf("valid JSON %q has rejected prefix %q: %v", value, value[:length], err)
			}
		}
	})
}
