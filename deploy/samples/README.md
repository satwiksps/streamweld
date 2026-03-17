# Deterministic vLLM-compatible sample

Install Streamweld in `streamweld-system`, then apply this directory:

```sh
kubectl create namespace streamweld-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/samples/deterministic-backend.yaml
kubectl apply -f deploy/samples/durability-policy.yaml
kubectl apply -f deploy/samples/inference-route.yaml
```

The backend implements the OpenAI chat-completions streaming shape and emits a
deterministic `token-000 …` sequence. It uses two CPU-only Python pods, so the
rollout and migration path can be exercised without a GPU. Its explicit
`preStop` drain hook and `terminationGracePeriodSeconds: 15` are the settings to
carry over to a real vLLM Deployment when the optional mutation webhook is off.
The sample intentionally omits the webhook opt-in label because it already owns
an equivalent dynamic pod-identity hook to the operator's all-proxy drain
Service; use `streamweld.io/managed: "true"` only on workloads whose first
container has no existing `preStop` action. A direct proxy ClusterIP hook is
not safe with multiple proxy replicas because it reaches only one process.
