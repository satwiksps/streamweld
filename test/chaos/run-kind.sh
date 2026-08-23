#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

for command in docker kind kubectl helm go curl; do
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
proxy_forward_pid=""
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
  capture_kubectl pods.yaml get pods --namespace streamweld-system -o yaml
  capture_kubectl pod-descriptions.txt describe pods --namespace streamweld-system
  capture_kubectl events.txt get events --all-namespaces --sort-by=.lastTimestamp
  capture_kubectl inferenceroutes.yaml get inferenceroutes.streamweld.io --namespace streamweld-system -o yaml
  capture_kubectl durabilitypolicies.yaml get durabilitypolicies.streamweld.io --namespace streamweld-system -o yaml

  helm status streamweld \
    --namespace streamweld-system \
    --kube-apiserver-timeout 15s \
    >"$diagnostics_dir/helm-status.txt" 2>&1 || true
  capture_component_logs proxy app.kubernetes.io/component=proxy
  capture_component_logs operator app.kubernetes.io/component=operator
  capture_component_logs backend app.kubernetes.io/name=streamweld-chaos-backend
  capture_component_logs redis app.kubernetes.io/component=redis

  for name in proxy backend; do
    if [[ -f "$forward_dir/$name.log" ]]; then
      cp "$forward_dir/$name.log" "$diagnostics_dir/port-forward-$name.log"
    fi
  done
}

cleanup() {
  for pid in "$proxy_forward_pid" "$backend_forward_pid"; do
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
  trap - EXIT
  set +e
  if (( status != 0 )); then
    collect_failure_diagnostics
  fi
  cleanup
  exit "$status"
}
trap on_exit EXIT

current_context="$(kubectl config current-context 2>/dev/null || true)"
if [[ -z "$cluster_name" && "$current_context" == kind-* ]] && kubectl cluster-info >/dev/null 2>&1; then
  cluster_name="${current_context#kind-}"
else
  cluster_name="${cluster_name:-streamweld-chaos}"
  if kind get clusters 2>/dev/null | grep -Fxq "$cluster_name"; then
    kubectl config use-context "kind-$cluster_name" >/dev/null
  else
    created_cluster=true
    kind create cluster --name "$cluster_name" --config test/chaos/kind-config.yaml --wait 120s
  fi
fi

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
kubectl apply -f test/chaos/manifests/backend.yaml
kubectl rollout status deployment/streamweld-chaos-backend --namespace streamweld-system --timeout=180s
helm upgrade --install streamweld deploy/helm/streamweld \
  --namespace streamweld-system \
  --create-namespace \
  --set proxy.replicaCount=2 \
  --set proxy.image.repository=streamweld-proxy \
  --set proxy.image.tag=chaos \
  --set proxy.image.pullPolicy=Never \
  --set proxy.backendURL=http://streamweld-chaos-backend:8000 \
  --set proxy.nodeSelector.streamweld-chaos-role=system \
  --set journal.backend=redis \
  --set reader.maxLagBytes=1024 \
  --set redis.enabled=true \
  --set operator.image.repository=streamweld-operator \
  --set operator.image.tag=chaos \
  --set operator.image.pullPolicy=Never \
  --set operator.nodeSelector.streamweld-chaos-role=system \
  --set-string operator.adminToken=streamweld-chaos-admin-token \
  --set redis.nodeSelector.streamweld-chaos-role=system \
  --wait \
  --timeout 180s

kubectl apply -f test/chaos/manifests/route.yaml
kubectl wait inferenceroute/deterministic-chaos \
  --namespace streamweld-system \
  --for=condition=Ready \
  --timeout=180s
kubectl wait inferenceroute/deterministic-chaos \
  --namespace streamweld-system \
  --for=jsonpath='{.status.healthyBackends}'=2 \
  --timeout=180s

mkdir -p "$forward_dir"
kubectl port-forward --namespace streamweld-system service/streamweld-proxy 18080:8080 >"$forward_dir/proxy.log" 2>&1 &
proxy_forward_pid=$!
kubectl port-forward --namespace streamweld-system service/streamweld-chaos-backend 18081:8000 >"$forward_dir/backend.log" 2>&1 &
backend_forward_pid=$!
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null && \
     curl --fail --silent http://127.0.0.1:18081/health >/dev/null; then
    break
  fi
  if ! kill -0 "$proxy_forward_pid" >/dev/null 2>&1 || ! kill -0 "$backend_forward_pid" >/dev/null 2>&1; then
    cat "$forward_dir/proxy.log" "$forward_dir/backend.log" >&2
    exit 1
  fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null
curl --fail --silent http://127.0.0.1:18081/health >/dev/null

go run ./cmd/streamweldctl bench \
  --profile kind \
  --proxy-url http://127.0.0.1:18080 \
  --direct-url http://127.0.0.1:18081 \
  --streams "${STREAMWELD_CHAOS_STREAMS:-8}" \
  --tokens "${STREAMWELD_CHAOS_TOKENS:-64}" \
  --output "$results_dir"
go run ./cmd/streamweldctl bench --verify --output "$results_dir"
