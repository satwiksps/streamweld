package proxy

import (
	"errors"
	"testing"
)

func TestSafeLogStringEscapesRecordBreaks(t *testing.T) {
	t.Parallel()

	if got, want := safeLogString("owner\r\nforged"), `owner\r\nforged`; got != want {
		t.Fatalf("safeLogString() = %q, want %q", got, want)
	}
	if got, want := safeLogError(errors.New("relay\nfailed")), `relay\nfailed`; got != want {
		t.Fatalf("safeLogError() = %q, want %q", got, want)
	}
}

func TestSafeLogErrorHandlesNil(t *testing.T) {
	t.Parallel()

	if got, want := safeLogError(nil), "<nil>"; got != want {
		t.Fatalf("safeLogError(nil) = %q, want %q", got, want)
	}
}
