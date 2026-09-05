package version

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestCurrent(t *testing.T) {
	t.Parallel()

	got := Current()
	if got.Version == "" || got.Commit == "" || got.Date == "" {
		t.Fatalf("Current() returned incomplete build identity: %#v", got)
	}
}

func TestInfoWrite(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		info Info
		want string
	}{
		{
			name: "release",
			info: Info{Version: "1.0.1", Commit: "abc123", Date: "2026-09-05T00:00:00Z"},
			want: "streamweld-proxy 1.0.1 (commit abc123, date 2026-09-05T00:00:00Z)\n",
		},
		{
			name: "development",
			info: Info{Version: "dev", Commit: "unknown", Date: "unknown"},
			want: "streamweld-proxy dev (commit unknown, date unknown)\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := test.info.Write(&output, "streamweld-proxy"); err != nil {
				t.Fatal(err)
			}
			if output.String() != test.want {
				t.Fatalf("version output = %q, want %q", output.String(), test.want)
			}
		})
	}
}

func TestInfoWriteReportsOutputFailure(t *testing.T) {
	t.Parallel()
	if err := Current().Write(closedVersionWriter{}, "streamweldctl"); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write error = %v, want closed output error", err)
	}
}

type closedVersionWriter struct{}

func (closedVersionWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
