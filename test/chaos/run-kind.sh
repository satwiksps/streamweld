#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

for command in docker kind kubectl helm go curl openssl sed timeout uname; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required kind-chaos command is missing: $command" >&2
    exit 1
  }
done

forward_dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/streamweld-chaos"
results_dir="${STREAMWELD_CHAOS_RESULTS_DIR:-$forward_dir/results}"
diagnostics_dir="$results_dir/diagnostics"
cluster_name="${KIND_CLUSTER_NAME:-}"
created_cluster=false
backend_forward_pid=""

capture_kubectl() {
  local output_file="$1"
  shift
  kubectl --request-timeout=15s "$@" >"$diagnostics_dir/$output_file" 2>&1 || true
}

capture_component_logs() {
  local component="$1"
  local selector="$2"
  capture_kubectl "$component.log" logs \
    --namespace streamweld-system \
    --selector "$selector" \
    --all-containers=true \
    --prefix=true \
    --tail=-1
  capture_kubectl "$component-previous.log" logs \
    --namespace streamweld-system \
    --selector "$selector" \
    --all-containers=true \
    --prefix=true \
    --tail=-1 \
    --previous
}

collect_failure_diagnostics() {
  mkdir -p "$diagnostics_dir"
  echo "kind chaos failed; collecting diagnostics in $diagnostics_dir" >&2

  {
    date --utc --iso-8601=seconds
    echo "cluster: ${cluster_name:-unknown}"
    echo "context: $(kubectl config current-context 2>/dev/null || echo unavailable)"
    kind get clusters 2>&1 || true
  } >"$diagnostics_dir/context.txt"

  capture_kubectl nodes.txt get nodes -o wide
  capture_kubectl pods-all-namespaces.txt get pods --all-namespaces -o wide
  capture_kubectl resources.txt get all --namespace streamweld-system -o wide
  capture_kubectl services.yaml get services --namespace streamweld-system -o yaml
  capture_kubectl endpointslices.yaml get endpointslices.discovery.k8s.io --namespace streamweld-system -o yaml
  capture_kubectl networkpolicies.yaml get networkpolicies.networking.k8s.io --namespace streamweld-system -o yaml
  capture_kubectl pods.yaml get pods --namespace streamweld-system -o yaml
  capture_kubectl pod-descriptions.txt describe pods --namespace streamweld-system
  capture_kubectl events.txt get events --all-namespaces --sort-by=.lastTimestamp
  capture_kubectl inferenceroutes.yaml get inferenceroutes.streamweld.io --namespace streamweld-system -o yaml
  capture_kubectl durabilitypolicies.yaml get durabilitypolicies.streamweld.io --namespace streamweld-system -o yaml
  capture_kubectl kube-proxy-config.yaml get configmap kube-proxy --namespace kube-system -o yaml
  capture_kubectl kube-proxy.log logs \
    --namespace kube-system \
    --selector k8s-app=kube-proxy \
    --all-containers=true \
    --prefix=true \
    --tail=-1
  capture_kubectl kindnet.log logs \
    --namespace kube-system \
    --selector k8s-app=kindnet \
    --all-containers=true \
    --prefix=true \
    --tail=-1

  timeout 20s helm status streamweld \
    --namespace streamweld-system \
    >"$diagnostics_dir/helm-status.txt" 2>&1 || true
  capture_component_logs proxy app.kubernetes.io/component=proxy
  capture_component_logs operator app.kubernetes.io/component=operator
  capture_component_logs backend app.kubernetes.io/name=streamweld-chaos-backend
  capture_component_logs redis app.kubernetes.io/component=redis

  if [[ -f "$forward_dir/backend.log" ]]; then
    cp "$forward_dir/backend.log" "$diagnostics_dir/port-forward-backend.log"
  fi
}

cleanup() {
  for pid in "$backend_forward_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$created_cluster" == true && "${STREAMWELD_CHAOS_KEEP_CLUSTER:-false}" != true ]]; then
    kind delete cluster --name "$cluster_name"
  fi
}

on_exit() {
  local status=$?
  local current_context=""
  trap - EXIT
  set +e
  if (( status != 0 )) && [[ "$created_cluster" == true ]]; then
    current_context="$(kubectl config current-context 2>/dev/null || true)"
    if [[ "$current_context" == "kind-$cluster_name" ]]; then
      collect_failure_diagnostics
    else
      echo "kind chaos failed before its dedicated context became active; skipping Kubernetes diagnostics" >&2
    fi
  fi
  cleanup
  exit "$status"
}
trap on_exit EXIT

if [[ "$(uname -s)" != Linux ]]; then
  echo "kind chaos requires a Linux host for direct, intermediary-free access to the kind NodePort" >&2
  exit 1
fi
cluster_name="${cluster_name:-streamweld-chaos}"
if kind get clusters 2>/dev/null | grep -Fxq "$cluster_name"; then
  echo "kind chaos requires a fresh dedicated cluster; delete $cluster_name or choose a new KIND_CLUSTER_NAME" >&2
  exit 1
fi
created_cluster=true
kind create cluster --name "$cluster_name" --config test/chaos/kind-config.yaml --wait 120s

# net.ipv4.tcp_wmem is a safe, pod-namespaced sysctl starting in Kubernetes
# 1.32 and requires Linux 4.15 or newer. Fail before image builds and Helm
# installation if the pinned cluster cannot honor the slow-reader contract.
kubelet_version="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}')"
if [[ ! "$kubelet_version" =~ ^v([0-9]+)\.([0-9]+)\. ]]; then
  echo "cannot determine Kubernetes version from kubelet version: $kubelet_version" >&2
  exit 1
fi
kubernetes_major="${BASH_REMATCH[1]}"
kubernetes_minor="${BASH_REMATCH[2]}"
if (( kubernetes_major < 1 || (kubernetes_major == 1 && kubernetes_minor < 32) )); then
  echo "kind chaos requires Kubernetes >=1.32 for pod-scoped net.ipv4.tcp_wmem; found $kubelet_version" >&2
  exit 1
fi
mapfile -t node_kernels < <(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"="}{.status.nodeInfo.kernelVersion}{"\n"}{end}')
for node_kernel in "${node_kernels[@]}"; do
  node_name="${node_kernel%%=*}"
  kernel_version="${node_kernel#*=}"
  if [[ ! "$kernel_version" =~ ^([0-9]+)\.([0-9]+) ]]; then
    echo "cannot determine Linux kernel version for $node_name: $kernel_version" >&2
    exit 1
  fi
  kernel_major="${BASH_REMATCH[1]}"
  kernel_minor="${BASH_REMATCH[2]}"
  if (( kernel_major < 4 || (kernel_major == 4 && kernel_minor < 15) )); then
    echo "kind chaos requires Linux >=4.15 for pod-scoped net.ipv4.tcp_wmem; $node_name runs $kernel_version" >&2
    exit 1
  fi
done

# kubeadm (and therefore kind) does not guarantee a positive worker-role label.
# Identify workers by excluding both current and legacy control-plane labels.
mapfile -t worker_nodes < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane,!node-role.kubernetes.io/master' -o name | sort)
if (( ${#worker_nodes[@]} < 3 )); then
  echo "kind chaos requires at least three worker nodes (two backend, one system); found ${#worker_nodes[@]}" >&2
  exit 1
fi
for index in "${!worker_nodes[@]}"; do
  role=system
  if (( index < 2 )); then
    role=backend
  fi
  kubectl label "${worker_nodes[$index]}" streamweld-chaos-role="$role" --overwrite >/dev/null
done

# Both proxy replicas are pinned to the dedicated system worker. Entering the
# local-only NodePort on that worker avoids a cross-node CNI hop while retaining
# end-to-end TCP backpressure for the slow-reader assertion.
proxy_node="${worker_nodes[2]#node/}"
proxy_node_ip="$(kubectl get node "$proxy_node" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
if [[ ! "$proxy_node_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
  echo "cannot determine the direct kind system-worker IPv4 address for $proxy_node: $proxy_node_ip" >&2
  exit 1
fi
proxy_url="http://${proxy_node_ip}:30080"
kind_gateway="$(docker inspect --format '{{(index .NetworkSettings.Networks "kind").Gateway}}' "$proxy_node")"
if [[ ! "$kind_gateway" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
  echo "cannot determine the kind Docker bridge IPv4 gateway: $kind_gateway" >&2
  exit 1
fi
kind_gateway_cidr="${kind_gateway}/32"

docker build --file test/e2e/Dockerfile --target proxy --tag streamweld-proxy:chaos .
docker build --file test/e2e/Dockerfile --target operator --tag streamweld-operator:chaos .
docker build --file test/chaos/Dockerfile --tag streamweld-chaos-backend:kind .
docker tag streamweld-chaos-backend:kind streamweld-chaos-backend:kind-rollout
kind load docker-image --name "$cluster_name" \
  streamweld-proxy:chaos \
  streamweld-operator:chaos \
  streamweld-chaos-backend:kind \
  streamweld-chaos-backend:kind-rollout

kubectl create namespace streamweld-system --dry-run=client -o yaml | kubectl apply -f -
relay_tls_dir="$forward_dir/relay-tls"
mkdir -p "$relay_tls_dir"
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$relay_tls_dir/ca.key" \
  -out "$relay_tls_dir/ca.crt" \
  -subj "/CN=streamweld-chaos-relay-ca" \
  -days 1 \
  -sha256 \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout "$relay_tls_dir/tls.key" \
  -out "$relay_tls_dir/tls.csr" \
  -subj "/CN=streamweld-relay.streamweld-system.svc" \
  -sha256 \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "subjectAltName=DNS:streamweld-relay.streamweld-system.svc" \
  -addext "extendedKeyUsage=serverAuth,clientAuth" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  >/dev/null 2>&1
openssl x509 -req \
  -in "$relay_tls_dir/tls.csr" \
  -CA "$relay_tls_dir/ca.crt" \
  -CAkey "$relay_tls_dir/ca.key" \
  -CAcreateserial \
  -out "$relay_tls_dir/tls.crt" \
  -days 1 \
  -sha256 \
  -copy_extensions copy \
  >/dev/null 2>&1
openssl verify \
  -CAfile "$relay_tls_dir/ca.crt" \
  -purpose sslserver \
  -verify_hostname streamweld-relay.streamweld-system.svc \
  "$relay_tls_dir/tls.crt"
openssl verify -CAfile "$relay_tls_dir/ca.crt" -purpose sslclient "$relay_tls_dir/tls.crt"
kubectl create secret generic streamweld-chaos-relay-tls \
  --namespace streamweld-system \
  --from-file=ca.crt="$relay_tls_dir/ca.crt" \
  --from-file=tls.crt="$relay_tls_dir/tls.crt" \
  --from-file=tls.key="$relay_tls_dir/tls.key"
kubectl apply -f test/chaos/manifests/backend.yaml
kubectl rollout status deployment/streamweld-chaos-backend --namespace streamweld-system --timeout=180s
# Ordinary 64-token scenarios stay below the reader budget during injection.
# The pod-scoped TCP send bound and 16-frame slow workload leave more than
# 64 KiB queued in byte-bounded reader delivery even with conservative
# read-ahead allowance.
helm upgrade --install streamweld deploy/helm/streamweld \
  --namespace streamweld-system \
  --create-namespace \
  --set proxy.replicaCount=2 \
  --set proxy.image.repository=streamweld-proxy \
  --set proxy.image.tag=chaos \
  --set proxy.image.pullPolicy=Never \
  --set proxy.backendURL=http://streamweld-chaos-backend:8000 \
  --set proxy.nodeSelector.streamweld-chaos-role=system \
  --set-string 'proxy.podSecurityContext.sysctls[0].name=net.ipv4.tcp_wmem' \
  --set-string 'proxy.podSecurityContext.sysctls[0].value=4096 4096 4096' \
  --set journal.backend=redis \
  --set reader.maxLagBytes=65536 \
  --set redis.enabled=true \
  --set relay.enabled=true \
  --set relay.tls.existingSecret=streamweld-chaos-relay-tls \
  --set operator.image.repository=streamweld-operator \
  --set operator.image.tag=chaos \
  --set operator.image.pullPolicy=Never \
  --set operator.nodeSelector.streamweld-chaos-role=system \
  --set-string operator.adminToken=streamweld-chaos-admin-token \
  --set redis.nodeSelector.streamweld-chaos-role=system \
  --wait \
  --timeout 180s

kubectl apply -f test/chaos/manifests/route.yaml
kubectl apply -f test/chaos/manifests/proxy-nodeport.yaml
# kind's default CNI enforces the chart NetworkPolicy. Add one disposable,
# source-specific rule for the runner host; the production policy remains
# installed and unchanged.
sed "s|__KIND_GATEWAY_CIDR__|${kind_gateway_cidr}|" \
  test/chaos/manifests/proxy-host-ingress.yaml.tmpl | kubectl apply -f -
kubectl wait inferenceroute/deterministic-chaos \
  --namespace streamweld-system \
  --for=condition=Ready \
  --timeout=180s
kubectl wait inferenceroute/deterministic-chaos \
  --namespace streamweld-system \
  --for=jsonpath='{.status.healthyBackends}'=2 \
  --timeout=180s

mkdir -p "$forward_dir"
kubectl port-forward --namespace streamweld-system service/streamweld-chaos-backend 18081:8000 >"$forward_dir/backend.log" 2>&1 &
backend_forward_pid=$!
for _ in $(seq 1 60); do
  if curl --fail --silent --show-error --connect-timeout 2 --max-time 3 "$proxy_url/healthz" >/dev/null && \
     curl --fail --silent --show-error --connect-timeout 2 --max-time 3 http://127.0.0.1:18081/health >/dev/null; then
    break
  fi
  if ! kill -0 "$backend_forward_pid" >/dev/null 2>&1; then
    cat "$forward_dir/backend.log" >&2
    exit 1
  fi
  sleep 1
done
if ! curl --fail --silent --show-error --connect-timeout 2 --max-time 3 "$proxy_url/healthz" >/dev/null; then
  echo "direct proxy NodePort is unreachable at $proxy_url; intermediary-free slow-reader testing is required" >&2
  exit 1
fi
curl --fail --silent --show-error --connect-timeout 2 --max-time 3 http://127.0.0.1:18081/health >/dev/null

go run ./cmd/streamweldctl bench \
  --profile kind \
  --proxy-url "$proxy_url" \
  --direct-url http://127.0.0.1:18081 \
  --streams "${STREAMWELD_CHAOS_STREAMS:-8}" \
  --tokens "${STREAMWELD_CHAOS_TOKENS:-64}" \
  --output "$results_dir"
go run ./cmd/streamweldctl bench --verify --output "$results_dir"
