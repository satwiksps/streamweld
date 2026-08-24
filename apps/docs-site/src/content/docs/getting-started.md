---
title: Install in ten minutes
description: Install Streamweld, start the deterministic sample backend, and open a durable stream.
---

# Install in ten minutes

## Prerequisites

- Kubernetes 1.27 or newer
- Helm 3.14 or newer
- `kubectl`
- Git

## Install the control and data planes

Clone the same release tag used by the chart. This supplies version-matched
sample manifests rather than relying on files from an unrelated checkout:

```sh
git clone --depth 1 --branch v0.1.0 https://github.com/satwiksps/streamweld.git
cd streamweld
```

```sh
helm upgrade --install streamweld oci://ghcr.io/satwiksps/charts/streamweld \
  --namespace streamweld-system \
  --create-namespace \
  --version 0.1.0 \
  --wait --timeout 3m
```

## Apply the CPU-only sample

```sh
kubectl apply -f deploy/samples/deterministic-backend.yaml
kubectl apply -f deploy/samples/durability-policy.yaml
kubectl apply -f deploy/samples/inference-route.yaml
kubectl -n streamweld-system scale deployment/streamweld-sample-backend --replicas=1
```

The sample does not need a GPU. Begin with one known origin backend, then wait
for that exact route state:

```sh
kubectl -n streamweld-system rollout status deployment/streamweld-sample-backend --timeout=180s
kubectl -n streamweld-system wait inferenceroute/deterministic-vllm \
  --for=condition=Ready --timeout=180s
kubectl -n streamweld-system wait inferenceroute/deterministic-vllm \
  --for=jsonpath='{.status.healthyBackends}'=1 --timeout=180s
```

## Open a durable stream

Port-forward the proxy in a second terminal:

```sh
kubectl -n streamweld-system port-forward service/streamweld-proxy 8080:8080
```

Then start an OpenAI-compatible request:

```sh
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"streamweld/deterministic-vllm","messages":[{"role":"user","content":"Count steadily."}],"max_tokens":2048,"stream":true}'
```

The response includes `X-Streamweld-Stream-Id` and numbered SSE events. A
disconnected reader resumes with `Last-Event-ID`; only the dedicated stop
endpoint cancels generation.

## Delete a backend while it is generating

Keep the request running. From another terminal, add a continuation target,
wait until the operator has admitted both Pods, then delete the older Pod—the
only backend that existed when the request began:

```sh
ORIGIN_POD=$(kubectl -n streamweld-system get pods \
  -l app.kubernetes.io/name=streamweld-sample-backend \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n streamweld-system scale deployment/streamweld-sample-backend --replicas=2
kubectl -n streamweld-system rollout status deployment/streamweld-sample-backend --timeout=180s
kubectl -n streamweld-system wait inferenceroute/deterministic-vllm \
  --for=jsonpath='{.status.healthyBackends}'=2 --timeout=180s
kubectl -n streamweld-system delete pod "$ORIGIN_POD" --wait=false
```

The `curl` process remains attached to the same logical stream. The proxy
journals the accepted prefix, selects another `SAFE` backend, reconciles the
continuation seam, and finishes the deterministic sequence without repeating an
accepted chunk.

!!! note "What this sample proves"

    The sample is CPU-only and deterministic, so it is suitable for protocol and
    rollout validation. It is not a performance proxy for vLLM or a
    production-model compatibility claim.

## Clean up

Delete the walkthrough namespace to remove the Helm release, control plane,
sample backend, CRs, and generated credentials together:

```sh
kubectl delete namespace streamweld-system
```
