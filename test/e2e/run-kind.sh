#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

for command in docker kind kubectl helm go curl; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required e2e command is missing: $command" >&2
    exit 1
  }
done

cluster_name="${KIND_CLUSTER_NAME:-}"
created_cluster=false
current_context="$(kubectl config current-context 2>/dev/null || true)"
if [[ -z "$cluster_name" && "$current_context" == kind-* ]] && kubectl cluster-info >/dev/null 2>&1; then
  cluster_name="${current_context#kind-}"
else
  cluster_name="${cluster_name:-streamweld-e2e}"
  if kind get clusters 2>/dev/null | grep -Fxq "$cluster_name"; then
    kubectl config use-context "kind-$cluster_name" >/dev/null
  else
    kind create cluster --name "$cluster_name" --config test/e2e/kind-config.yaml --wait 120s
    created_cluster=true
  fi
fi

port_forward_pid=""
dump_diagnostics() {
  if ! kubectl cluster-info >/dev/null 2>&1; then
    return
  fi

  echo "::group::Streamweld kind diagnostics" >&2
  kubectl get deployments,pods,services --namespace streamweld-system -o wide >&2 || true
  kubectl describe deployment/streamweld-proxy --namespace streamweld-system >&2 || true
  kubectl describe pods --namespace streamweld-system >&2 || true
  kubectl get events --namespace streamweld-system --sort-by=.lastTimestamp >&2 || true

  while IFS= read -r pod; do
    [[ -n "$pod" ]] || continue
    echo "--- logs: $pod ---" >&2
    kubectl logs "$pod" --namespace streamweld-system --all-containers --tail=-1 >&2 || true
    echo "--- previous logs: $pod ---" >&2
    kubectl logs "$pod" --namespace streamweld-system --all-containers --previous --tail=-1 >&2 || true
  done < <(kubectl get pods --namespace streamweld-system -o name 2>/dev/null || true)
  echo "::endgroup::" >&2
}

cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )); then
    dump_diagnostics
  fi
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$created_cluster" == true && "${STREAMWELD_E2E_KEEP_CLUSTER:-false}" != true ]]; then
    kind delete cluster --name "$cluster_name"
  fi
  exit "$status"
}
trap cleanup EXIT

docker build --file test/e2e/Dockerfile --target proxy --tag streamweld-proxy:e2e .
docker build --file test/e2e/Dockerfile --target operator --tag streamweld-operator:e2e .
kind load docker-image --name "$cluster_name" streamweld-proxy:e2e streamweld-operator:e2e

kubectl create namespace streamweld-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/samples/deterministic-backend.yaml
kubectl rollout status deployment/streamweld-sample-backend --namespace streamweld-system --timeout=180s

helm upgrade --install streamweld deploy/helm/streamweld \
  --namespace streamweld-system \
  --create-namespace \
  --set proxy.replicaCount=2 \
  --set proxy.image.repository=streamweld-proxy \
  --set proxy.image.tag=e2e \
  --set proxy.image.pullPolicy=Never \
  --set proxy.backendURL=http://streamweld-sample-backend:8000 \
  --set migration.stallDetection.enabled=false \
  --set journal.backend=redis \
  --set redis.enabled=true \
  --set operator.image.repository=streamweld-operator \
  --set operator.image.tag=e2e \
  --set operator.image.pullPolicy=Never \
  --set-string operator.adminToken=streamweld-e2e-admin-token \
  --wait \
  --timeout 180s

kubectl apply -f deploy/samples/durability-policy.yaml
kubectl apply -f deploy/samples/inference-route.yaml
kubectl wait inferenceroute/deterministic-vllm \
  --namespace streamweld-system \
  --for=condition=Ready \
  --timeout=180s

helm test streamweld --namespace streamweld-system --timeout 60s

port_forward_log="${TMPDIR:-/tmp}/streamweld-e2e-port-forward.log"
kubectl port-forward --namespace streamweld-system service/streamweld-proxy 18080:8080 >"$port_forward_log" 2>&1 &
port_forward_pid=$!
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then
    break
  fi
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    cat "$port_forward_log" >&2
    exit 1
  fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null || {
  cat "$port_forward_log" >&2
  exit 1
}

STREAMWELD_E2E_CLUSTER=1 \
STREAMWELD_E2E_PROXY_URL=http://127.0.0.1:18080 \
STREAMWELD_E2E_NAMESPACE=streamweld-system \
go test -race ./test/e2e/... -count=1 -timeout=6m
