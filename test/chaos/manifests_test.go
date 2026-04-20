package chaos

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestKindManifestsAreValidMultiDocumentYAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path      string
		documents int
	}{
		{path: "kind-config.yaml", documents: 1},
		{path: filepath.Join("manifests", "backend.yaml"), documents: 2},
		{path: filepath.Join("manifests", "route.yaml"), documents: 2},
	}
	for _, test := range tests {
		file, err := os.Open(test.path)
		if err != nil {
			t.Fatalf("open %s: %v", test.path, err)
		}
		decoder := utilyaml.NewYAMLOrJSONDecoder(file, 64<<10)
		documents := 0
		for {
			var value map[string]any
			err := decoder.Decode(&value)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = file.Close()
				t.Fatalf("decode %s document %d: %v", test.path, documents+1, err)
			}
			if len(value) != 0 {
				documents++
			}
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close %s: %v", test.path, err)
		}
		if documents != test.documents {
			t.Errorf("%s documents = %d, want %d", test.path, documents, test.documents)
		}
	}
}

func TestKindTopologyIsolatesPhysicalBackendDisruptions(t *testing.T) {
	t.Parallel()

	kindConfig, err := os.ReadFile("kind-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(kindConfig), "role: worker"); got != 3 {
		t.Fatalf("kind worker count = %d, want 3", got)
	}
	if got := strings.Count(string(kindConfig), "streamweld-chaos-role: backend"); got != 2 {
		t.Fatalf("kind backend worker labels = %d, want 2", got)
	}
	if got := strings.Count(string(kindConfig), "streamweld-chaos-role: system"); got != 1 {
		t.Fatalf("kind system worker labels = %d, want 1", got)
	}

	backend, err := os.ReadFile(filepath.Join("manifests", "backend.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	backendText := strings.ReplaceAll(string(backend), "\r\n", "\n")
	if !strings.Contains(backendText, "nodeSelector:\n        streamweld-chaos-role: backend") {
		t.Fatal("backend Deployment is not pinned to the two disruption workers")
	}
	route, err := os.ReadFile(filepath.Join("manifests", "route.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(route), "seamWindowBytes: 64") {
		t.Fatalf("kind policy does not match deterministic seam window %d", DeterministicSeamWindowBytes)
	}
}
