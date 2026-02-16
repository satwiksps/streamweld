package migrate

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestReconcileSeamOverlapBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		accumulated  string
		continuation string
		window       int
		wantContent  string
		wantOverlap  int
	}{
		{
			name:         "zero overlap",
			accumulated:  "hello there",
			continuation: "general kenobi",
			window:       64,
			wantContent:  "general kenobi",
		},
		{
			name:         "partial window overlap",
			accumulated:  "hello world",
			continuation: "world again",
			window:       64,
			wantContent:  " again",
			wantOverlap:  len("world"),
		},
		{
			name:         "full continuation window overlap",
			accumulated:  "prefix-abcdefgh",
			continuation: "abcdefgh-tail",
			window:       8,
			wantContent:  "-tail",
			wantOverlap:  8,
		},
		{
			name:         "longest repeated overlap wins",
			accumulated:  "start-ababab",
			continuation: "ababab-end",
			window:       64,
			wantContent:  "-end",
			wantOverlap:  6,
		},
		{
			name:         "multibyte overlap counts bytes",
			accumulated:  "snow 雪",
			continuation: "雪 falls",
			window:       64,
			wantContent:  " falls",
			wantOverlap:  len("雪"),
		},
		{
			name:         "window inside rune inspects prior boundary",
			accumulated:  "a雪",
			continuation: "雪z",
			window:       1,
			wantContent:  "雪z",
		},
		{
			name:         "empty rewritten continuation",
			accumulated:  "entire duplicate",
			continuation: "entire duplicate",
			window:       64,
			wantContent:  "",
			wantOverlap:  len("entire duplicate"),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := ReconcileSeam([]byte(test.accumulated), []byte(test.continuation), test.window)
			if err != nil {
				t.Fatalf("ReconcileSeam() error = %v", err)
			}
			if string(result.Content) != test.wantContent || result.OverlapBytes != test.wantOverlap {
				t.Fatalf("ReconcileSeam() = content %q, overlap %d; want %q, %d", result.Content, result.OverlapBytes, test.wantContent, test.wantOverlap)
			}
			if result.Anomaly {
				t.Fatalf("ReconcileSeam() unexpectedly reported anomaly")
			}
			if !utf8.Valid(result.Content) {
				t.Fatalf("ReconcileSeam() produced invalid UTF-8 %x", result.Content)
			}
		})
	}
}

func TestReconcileSeamNeverUsesOverlapBeyondWindow(t *testing.T) {
	t.Parallel()

	result, err := ReconcileSeam([]byte("prefixXYZ"), []byte("XYZrest"), 2)
	if err != nil {
		t.Fatalf("ReconcileSeam() error = %v", err)
	}
	if result.OverlapBytes != 0 || string(result.Content) != "XYZrest" {
		t.Fatalf("ReconcileSeam() = %#v, want no overlap beyond window", result)
	}
}

func TestReconcileSeamAnomalyDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		accumulated  string
		continuation string
		want         bool
	}{
		{name: "uppercase restart", accumulated: "the capital is", continuation: "Paris", want: true},
		{name: "trim whitespace", accumulated: "  incomplete,\t", continuation: "  Next", want: true},
		{name: "lowercase continuation", accumulated: "the capital is", continuation: "paris"},
		{name: "empty accumulated", continuation: "Paris"},
		{name: "empty continuation", accumulated: "incomplete"},
		{name: "period terminal", accumulated: "Finished.", continuation: "Next"},
		{name: "exclamation terminal", accumulated: "Finished!", continuation: "Next"},
		{name: "question terminal", accumulated: "Finished?", continuation: "Next"},
		{name: "ellipsis terminal", accumulated: "Finished…", continuation: "Next"},
		{name: "terminal closing quote", accumulated: `He said "finished."`, continuation: "Next"},
		{name: "terminal nested closers", accumulated: "Finished!”)]", continuation: "Next"},
		{name: "unicode titlecase is not uppercase", accumulated: "incomplete", continuation: "ǅuro"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := ReconcileSeam([]byte(test.accumulated), []byte(test.continuation), 64)
			if err != nil {
				t.Fatalf("ReconcileSeam() error = %v", err)
			}
			if result.Anomaly != test.want {
				t.Fatalf("Anomaly = %v, want %v; result %#v", result.Anomaly, test.want, result)
			}
		})
	}

	overlap, err := ReconcileSeam([]byte("prefix Paris"), []byte("Paris continues"), 64)
	if err != nil {
		t.Fatalf("ReconcileSeam(overlap) error = %v", err)
	}
	if overlap.Anomaly || overlap.OverlapBytes == 0 {
		t.Fatalf("real overlap reported anomaly: %#v", overlap)
	}
}

func TestReconcileSeamRejectsUnsafeInputs(t *testing.T) {
	t.Parallel()

	if _, err := ReconcileSeam([]byte("a"), []byte("b"), 0); !errors.Is(err, ErrInvalidSeamWindow) {
		t.Fatalf("zero window error = %v", err)
	}
	if _, err := ReconcileSeam([]byte{0xff}, []byte("b"), 64); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("invalid accumulated UTF-8 error = %v", err)
	}
	if _, err := ReconcileSeam([]byte("a"), []byte{0xe9, 0x9b}, 64); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("partial continuation UTF-8 error = %v", err)
	}
}

func FuzzReconcileSeamProperties(f *testing.F) {
	f.Add("hello world", "world again", uint8(64))
	f.Add("snow 雪", "雪 falls", uint8(4))
	f.Add(strings.Repeat("a", 80), strings.Repeat("a", 64)+"b", uint8(64))
	f.Fuzz(func(t *testing.T, accumulated, continuation string, rawWindow uint8) {
		if !utf8.ValidString(accumulated) || !utf8.ValidString(continuation) {
			return
		}
		window := 1 + int(rawWindow%128)
		result, err := ReconcileSeam([]byte(accumulated), []byte(continuation), window)
		if err != nil {
			t.Fatalf("ReconcileSeam() error = %v", err)
		}
		if result.OverlapBytes < 0 || result.OverlapBytes > window || result.OverlapBytes > len(continuation) || result.OverlapBytes > len(accumulated) {
			t.Fatalf("invalid overlap %d for window %d, accumulated %d, continuation %d", result.OverlapBytes, window, len(accumulated), len(continuation))
		}
		if !utf8.ValidString(accumulated[len(accumulated)-result.OverlapBytes:]) || !utf8.ValidString(continuation[result.OverlapBytes:]) {
			t.Fatalf("overlap split a UTF-8 rune: overlap %d", result.OverlapBytes)
		}
		if !bytes.Equal([]byte(accumulated[len(accumulated)-result.OverlapBytes:]), []byte(continuation[:result.OverlapBytes])) {
			t.Fatalf("reported overlap bytes do not match")
		}
		if !bytes.Equal(result.Content, []byte(continuation[result.OverlapBytes:])) {
			t.Fatalf("content %x is not continuation after overlap", result.Content)
		}
		if !utf8.Valid(result.Content) {
			t.Fatalf("result content is invalid UTF-8: %x", result.Content)
		}
	})
}
