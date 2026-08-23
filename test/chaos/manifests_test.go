package chaos

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/labels"
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

func TestKindRunnerDoesNotRequireOptionalWorkerRoleLabel(t *testing.T) {
	t.Parallel()

	const workerSelector = "!node-role.kubernetes.io/control-plane,!node-role.kubernetes.io/master"
	runner, err := os.ReadFile("run-kind.sh")
	if err != nil {
		t.Fatal(err)
	}
	runnerText := string(runner)
	if strings.Contains(runnerText, "-l 'node-role.kubernetes.io/worker'") {
		t.Fatal("kind runner requires the optional Kubernetes worker-role label")
	}
	if !strings.Contains(runnerText, "-l '"+workerSelector+"'") {
		t.Fatal("kind runner does not discover workers by excluding control-plane nodes")
	}

	selector, err := labels.Parse(workerSelector)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		labels map[string]string
		worker bool
	}{
		{name: "unlabelled worker", labels: nil, worker: true},
		{name: "current control plane", labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}},
		{name: "legacy control plane", labels: map[string]string{"node-role.kubernetes.io/master": ""}},
	}
	for _, test := range tests {
		if got := selector.Matches(labels.Set(test.labels)); got != test.worker {
			t.Errorf("selector match for %s = %t, want %t", test.name, got, test.worker)
		}
	}
}

func TestKindRunnerPreservesFailureDiagnostics(t *testing.T) {
	t.Parallel()

	runner, err := os.ReadFile("run-kind.sh")
	if err != nil {
		t.Fatal(err)
	}
	runnerText := strings.ReplaceAll(string(runner), "\r\n", "\n")
	for _, required := range []string{
		"collect_failure_diagnostics",
		"capture_kubectl resources.txt get all --namespace streamweld-system -o wide",
		"capture_kubectl events.txt get events --all-namespaces --sort-by=.lastTimestamp",
		"capture_component_logs proxy app.kubernetes.io/component=proxy",
		"capture_component_logs operator app.kubernetes.io/component=operator",
		"capture_component_logs backend app.kubernetes.io/name=streamweld-chaos-backend",
		"capture_component_logs redis app.kubernetes.io/component=redis",
		"if (( status != 0 )); then\n    collect_failure_diagnostics\n  fi\n  cleanup\n  exit \"$status\"",
	} {
		if !strings.Contains(runnerText, required) {
			t.Errorf("kind runner is missing failure-diagnostic contract %q", required)
		}
	}

	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "nightly.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(workflow), "${{ runner.temp }}/streamweld-kind-results/diagnostics") {
		t.Fatal("nightly workflow does not upload kind failure diagnostics")
	}
}
