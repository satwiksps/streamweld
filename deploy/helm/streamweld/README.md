# Streamweld Helm chart

The chart installs the Streamweld proxy and operator, including its CRDs. The
default in-memory journal deliberately runs one proxy replica:

```sh
helm install streamweld deploy/helm/streamweld \
  --namespace streamweld-system --create-namespace
```

Install a multi-replica deployment with the dependency-free embedded Redis:

```sh
helm upgrade --install streamweld deploy/helm/streamweld \
  --namespace streamweld-system --create-namespace \
  --set journal.backend=redis \
  --set redis.enabled=true \
  --set proxy.replicaCount=2
```

Rendering fails if `proxy.replicaCount` or the HPA can exceed one while the
journal is `memory`. For production, prefer an externally managed Redis and
provide its URL through `journal.redis.existingSecret`; the selected key is
`journal.redis.secretKey` (`redis-url` by default).

The owner relay is off by default. When enabled, each Deployment pod advertises
its pod-specific name beneath the relay headless Service. Supply
`relay.tls.existingSecret` with `ca.crt`, `tls.crt`, and `tls.key`; certificates
must cover the per-pod wildcard
`*.<release>-streamweld-relay.<namespace>.svc`. Relay ingress is restricted to
the release's proxy pods. Public/admin HTTP and the unauthenticated backend
drain hook are namespace-restricted by default; add an ingress-controller
namespace label map under `relay.networkPolicy.publicIngressNamespaceSelector`
when traffic enters from another namespace.

Pod mutation is also disabled by default. Enable it only with either an
existing serving certificate and CA bundle or cert-manager issuer settings.
The webhook matches pods explicitly labeled `streamweld.io/managed: "true"`
and uses `failurePolicy: Ignore` unless configured otherwise.

The operator exposes a namespace-private drain Service on port 8082. A backend
preStop request to that Service is fanned out to every non-terminating proxy
EndpointSlice member and succeeds only after all replicas acknowledge zero
in-flight streams for the pod. Discovery fails closed when there are no proxy
endpoints. Do not point HA backend hooks at the proxy
ClusterIP: a ClusterIP request reaches only one proxy process. Because a
Kubernetes HTTP lifecycle hook cannot attach a bearer token, the chart requires
its namespace-scoped NetworkPolicy while the operator drain listener is on.
The operator authenticates every downstream per-proxy drain request with the
same release-scoped bearer token used for route programming.
For backend pods outside the release namespace, set
`operator.drain.allowedNamespaceSelector` to labels shared by only the intended
backend namespaces.

The sample backend is CPU-only but vLLM/OpenAI-compatible:

```sh
kubectl apply -f deploy/samples/deterministic-backend.yaml
kubectl apply -f deploy/samples/durability-policy.yaml
kubectl apply -f deploy/samples/inference-route.yaml
```

Its Deployment demonstrates the recommended backend rollout setting:
`terminationGracePeriodSeconds: 15`, paired with a `preStop` call to the
operator drain Service. Run `make e2e` to build local proxy/operator images,
install the chart on kind, roll the sample backend, and verify every
deterministic stream byte-for-byte.
