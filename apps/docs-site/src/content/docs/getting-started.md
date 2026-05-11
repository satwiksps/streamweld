---
title: Install in ten minutes
description: Install Streamweld, start the deterministic sample backend, and open a durable stream.
---

## Prerequisites

- Kubernetes 1.27 or newer
- Helm 3.14 or newer
- `kubectl`

## Install the control and data planes

```sh
helm upgrade --install streamweld oci://ghcr.io/streamweld/charts/streamweld \
  --namespace streamweld-system \
  --create-namespace
```

For a checkout that has not published a release yet, use the local chart path:

```sh
helm upgrade --install streamweld ./deploy/helm/streamweld \
  --namespace streamweld-system \
  --create-namespace
```

## Apply the CPU-only sample

```sh
kubectl apply -f deploy/samples/deterministic-backend.yaml
kubectl apply -f deploy/samples/durability-policy.yaml
kubectl apply -f deploy/samples/inference-route.yaml
```

The sample does not need a GPU. Wait for the backend and Streamweld pods:

```sh
kubectl wait --for=condition=Available deployment \
  --all --all-namespaces --timeout=180s
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
  -d '{"model":"deterministic","messages":[{"role":"user","content":"Count steadily."}],"stream":true}'
```

The response includes `X-Streamweld-Stream-Id` and numbered SSE events. A
disconnected reader resumes with `Last-Event-ID`; only the dedicated stop
endpoint cancels generation.

## Delete a backend while it is generating

Keep the request running and, from another terminal, delete one selected
backend Pod:

```sh
kubectl delete pod -n streamweld-system \
  -l app.kubernetes.io/name=streamweld-sample-backend \
  --field-selector=status.phase=Running \
  --wait=false
```

The `curl` process remains attached to the same logical stream. The proxy
journals the accepted prefix, selects another `SAFE` backend, reconciles the
continuation seam, and finishes the deterministic sequence without repeating an
accepted chunk.

:::note[What this sample proves]
The sample is CPU-only and deterministic, so it is suitable for protocol and
rollout validation. It is not a performance proxy for vLLM or a production-model
compatibility claim.
:::

## Clean up

```sh
kubectl delete -f deploy/samples/inference-route.yaml
kubectl delete -f deploy/samples/durability-policy.yaml
kubectl delete -f deploy/samples/deterministic-backend.yaml
helm uninstall streamweld --namespace streamweld-system
kubectl delete namespace streamweld-system
```
