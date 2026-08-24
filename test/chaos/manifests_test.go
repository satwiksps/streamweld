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
		{path: filepath.Join("manifests", "proxy-nodeport.yaml"), documents: 1},
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

func TestKindSlowConsumerUsesDirectNodePort(t *testing.T) {
	t.Parallel()

	kindConfig, err := os.ReadFile("kind-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	kindText := strings.ReplaceAll(string(kindConfig), "\r\n", "\n")
	if strings.Contains(kindText, "extraPortMappings:") {
		t.Fatal("kind config still inserts Docker's buffered published-port path")
	}

	service, err := os.ReadFile(filepath.Join("manifests", "proxy-nodeport.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	serviceText := strings.ReplaceAll(string(service), "\r\n", "\n")
	for _, required := range []string{"type: NodePort", "externalTrafficPolicy: Local", "nodePort: 30080"} {
		if !strings.Contains(serviceText, required) {
			t.Errorf("chaos proxy Service is missing %q", required)
		}
	}
	if strings.Contains(serviceText, "kind: NetworkPolicy") {
		t.Fatal("chaos NodePort manifest embeds the separately scoped host-ingress policy")
	}

	hostIngress, err := os.ReadFile(filepath.Join("manifests", "proxy-host-ingress.yaml.tmpl"))
	if err != nil {
		t.Fatal(err)
	}
	hostIngressText := strings.ReplaceAll(string(hostIngress), "\r\n", "\n")
	for _, required := range []string{
		"name: streamweld-proxy-chaos-host-ingress",
		"app.kubernetes.io/name: streamweld",
		"app.kubernetes.io/component: proxy",
		"app.kubernetes.io/instance: streamweld",
		"cidr: __KIND_GATEWAY_CIDR__",
		"port: http",
	} {
		if !strings.Contains(hostIngressText, required) {
			t.Errorf("chaos host-ingress template is missing %q", required)
		}
	}
	if strings.Contains(hostIngressText, "0.0.0.0/0") {
		t.Fatal("chaos host-ingress policy permits every IPv4 source")
	}

	runner, err := os.ReadFile("run-kind.sh")
	if err != nil {
		t.Fatal(err)
	}
	runnerText := string(runner)
	if !strings.Contains(runnerText, "kubectl apply -f test/chaos/manifests/proxy-nodeport.yaml") {
		t.Fatal("kind runner does not install the direct proxy NodePort")
	}
	for _, required := range []string{
		`proxy_node="${worker_nodes[2]#node/}"`,
		`kubectl get node "$proxy_node" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'`,
		`proxy_url="http://${proxy_node_ip}:30080"`,
		`kind_gateway="$(docker inspect --format '{{(index .NetworkSettings.Networks "kind").Gateway}}' "$proxy_node")"`,
		`kind_gateway_cidr="${kind_gateway}/32"`,
		`sed "s|__KIND_GATEWAY_CIDR__|${kind_gateway_cidr}|"`,
		`--proxy-url "$proxy_url"`,
	} {
		if !strings.Contains(runnerText, required) {
			t.Errorf("kind runner is missing intermediary-free proxy routing %q", required)
		}
	}
	if !strings.Contains(runnerText, "--set reader.maxLagBytes=65536") {
		t.Fatal("kind runner reader budget no longer matches the deterministic slow-consumer workload")
	}
	for _, required := range []string{
		"proxy.podSecurityContext.sysctls[0].name=net.ipv4.tcp_wmem",
		"proxy.podSecurityContext.sysctls[0].value=4096 4096 4096",
	} {
		if !strings.Contains(runnerText, required) {
			t.Errorf("kind runner is missing bounded proxy TCP send memory %q", required)
		}
	}
	for _, required := range []string{
		`basicConstraints=critical,CA:FALSE`,
		`subjectAltName=DNS:streamweld-relay.streamweld-system.svc`,
		`extendedKeyUsage=serverAuth,clientAuth`,
		`-verify_hostname streamweld-relay.streamweld-system.svc`,
		"kubectl create secret generic streamweld-chaos-relay-tls",
		"--set relay.enabled=true",
		"--set relay.tls.existingSecret=streamweld-chaos-relay-tls",
	} {
		if !strings.Contains(runnerText, required) {
			t.Errorf("kind runner is missing owner-relay fixture %q", required)
		}
	}
	if strings.Contains(runnerText, "service/streamweld-proxy 18080:8080") {
		t.Fatal("kind runner still places kubectl port-forward buffering in the slow-reader path")
	}
	if strings.Contains(runnerText, "127.0.0.1:18080") {
		t.Fatal("kind runner still uses a Docker-published proxy port")
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
	const kindNodeImage = "kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f"
	if got := strings.Count(string(kindConfig), kindNodeImage); got != 4 {
		t.Fatalf("pinned kind node image count = %d, want 4", got)
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
		`[[ "$created_cluster" == true ]]`,
		`[[ "$current_context" == "kind-$cluster_name" ]]`,
		"kind chaos requires a fresh dedicated cluster",
		"kind chaos requires Kubernetes >=1.32 for pod-scoped net.ipv4.tcp_wmem",
		"kind chaos requires Linux >=4.15 for pod-scoped net.ipv4.tcp_wmem",
		"capture_kubectl resources.txt get all --namespace streamweld-system -o wide",
		"capture_kubectl services.yaml get services --namespace streamweld-system -o yaml",
		"capture_kubectl endpointslices.yaml get endpointslices.discovery.k8s.io --namespace streamweld-system -o yaml",
		"capture_kubectl networkpolicies.yaml get networkpolicies.networking.k8s.io --namespace streamweld-system -o yaml",
		"capture_kubectl events.txt get events --all-namespaces --sort-by=.lastTimestamp",
		"capture_kubectl kube-proxy-config.yaml get configmap kube-proxy --namespace kube-system -o yaml",
		"capture_kubectl kube-proxy.log logs",
		"capture_kubectl kindnet.log logs",
		"capture_component_logs proxy app.kubernetes.io/component=proxy",
		"capture_component_logs operator app.kubernetes.io/component=operator",
		"capture_component_logs backend app.kubernetes.io/name=streamweld-chaos-backend",
		"capture_component_logs redis app.kubernetes.io/component=redis",
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
