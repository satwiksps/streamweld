package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/satwiksps/streamweld/internal/version"
)

func TestVersionReportsBuildIdentity(t *testing.T) {
	t.Parallel()
	for _, argument := range []string{"--version", "-version"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{argument}, &stdout, &stderr); code != 0 {
			t.Fatalf("version exit = %d, stderr = %q", code, stderr.String())
		}
		for _, field := range []string{"streamweldctl " + version.Version, "commit " + version.Commit, "date " + version.Date} {
			if !strings.Contains(stdout.String(), field) {
				t.Errorf("version output %q does not contain %q", stdout.String(), field)
			}
		}
		if stderr.Len() != 0 || strings.Count(stdout.String(), "\n") != 1 {
			t.Fatalf("stdout/stderr = %q/%q, want one version line only", stdout.String(), stderr.String())
		}
	}
}

func TestVersionReportsOutputFailure(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	if code := run([]string{"--version"}, closedVersionWriter{}, &stderr); code != 1 {
		t.Fatalf("version write failure exit = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "write version") {
		t.Fatalf("missing version output error: %q", stderr.String())
	}
}

func TestVersionRejectsUnexpectedArguments(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--version", "unexpected"}, &stdout, &stderr); code != 2 || stdout.Len() != 0 {
		t.Fatalf("version misuse exit/stdout/stderr = %d/%q/%q", code, stdout.String(), stderr.String())
	}
}

type closedVersionWriter struct{}

func (closedVersionWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
