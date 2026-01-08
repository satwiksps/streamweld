// Package version exposes build metadata shared by Streamweld binaries.
package version

var (
	// Version is replaced by release builds. Development builds use "dev".
	Version = "dev"
	// Commit is replaced by release builds when the Git revision is known.
	Commit = "unknown"
	// Date is replaced by release builds with an RFC 3339 build time.
	Date = "unknown"
)

// Info is the serializable build identity of a Streamweld binary.
type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

// Current returns the build identity embedded in the binary.
func Current() Info {
	return Info{Version: Version, Commit: Commit, Date: Date}
}
