package version

import "testing"

func TestCurrent(t *testing.T) {
	t.Parallel()

	got := Current()
	if got.Version == "" || got.Commit == "" || got.Date == "" {
		t.Fatalf("Current() returned incomplete build identity: %#v", got)
	}
}
