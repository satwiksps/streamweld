# Streamweld Helm chart

The chart installs the Streamweld proxy and operator, including its CRDs. The
default in-memory journal deliberately runs one proxy replica. Install the
published chart from GitHub Container Registry:

```sh
helm upgrade --install streamweld oci://ghcr.io/satwiksps/charts/streamweld \
  --namespace streamweld-system \
  --create-namespace \
  --version 1.0.0 \
  --wait --timeout 3m
```

For development from a source checkout:

```sh
helm install streamweld deploy/helm/streamweld \
  --namespace streamweld-system --create-namespace
```

Install a multi-replica development deployment with the embedded Redis:

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
Memory-mode proxies and embedded Redis use `Recreate` updates so a Service
never splits traffic between independent journals. Updates briefly interrupt
availability, and memory journals or embedded Redis without persistence lose
their retained data on replacement. Redis-backed proxies retain rolling updates.

The owner relay is off by default. When enabled, each proxy advertises its Pod
IP and verifies peers against the release's relay Service identity. Supply
`relay.tls.existingSecret` with `ca.crt`, `tls.crt`, and `tls.key`; the shared
certificate must cover
the rendered `<fullname>-relay.<namespace>.svc` Service DNS name (for the
documented install, `streamweld-relay.streamweld-system.svc`). The process
generates a new replica identity on every start so a restarted container cannot
renew stale ownership. The Service name is a TLS identity, not the owner route;
relay traffic goes directly to the advertised Pod IP. Relay ingress is
restricted to the release's proxy pods.
Public/admin HTTP and the unauthenticated backend
drain hook are namespace-restricted by default; add an ingress-controller
namespace label map under `relay.networkPolicy.publicIngressNamespaceSelector`
when traffic enters from another namespace.

Pod mutation is also disabled by default. Enable it only with either an
existing serving certificate and CA bundle or cert-manager issuer settings.
The webhook matches pods explicitly labeled `streamweld.io/managed: "true"`
and uses `failurePolicy: Ignore` unless configured otherwise.
Injected hooks use Kubernetes `httpGet`; the operator also accepts `POST` for
the CLI and explicit exec hooks. Kubelet executes HTTP lifecycle hooks from the
node, so node DNS and networking must reach the configured operator Service.
Use an in-container exec hook as in the sample backend when nodes cannot
resolve Service DNS names.

The operator exposes a namespace-private drain Service on port 8082. A backend
preStop request to that Service is fanned out to every non-terminating proxy
EndpointSlice member and succeeds only after all replicas acknowledge zero
in-flight streams for the pod. Discovery fails closed when there are no proxy
endpoints. Do not point HA backend hooks at the proxy
ClusterIP: a ClusterIP request reaches only one proxy process. Because
Kubernetes HTTP lifecycle headers cannot reference Secrets, the injected hook
avoids placing the admin token in the Pod specification. The chart requires
its namespace-scoped NetworkPolicy while the operator drain listener is on.
The operator authenticates every downstream per-proxy drain request with the
same release-scoped bearer token used for route programming.
The complete drain fanout, including endpoint discovery and queued replicas,
is bounded by `operator.drain.timeout` plus two seconds for response delivery.
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
