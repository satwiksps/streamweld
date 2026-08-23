package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/apis/v1alpha1"
	"github.com/satwiksps/streamweld/internal/conformance"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestInferenceRouteReadyTransitionProbesThenPushesCompleteSet(t *testing.T) {
	t.Parallel()
	fixture := newRouteFixture(t, false, conformance.VerdictSafe)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}}

	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if calls := fixture.checker.callCount(); calls != 0 {
		t.Fatalf("probe calls before Ready = %d", calls)
	}
	first := fixture.admin.lastSnapshot(t)
	if len(first.Backends) != 0 {
		t.Fatalf("not-Ready backend was admitted: %#v", first.Backends)
	}

	fixture.setEndpointReady(t, true)
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if calls := fixture.checker.callCount(); calls != 1 {
		t.Fatalf("probe calls after Ready = %d, want 1", calls)
	}
	snapshot := fixture.admin.lastSnapshot(t)
	if len(snapshot.Backends) != 1 {
		t.Fatalf("backend set = %#v", snapshot.Backends)
	}
	backend := snapshot.Backends[0]
	if backend.ID != "pod-uid-1" || backend.URL != "http://10.0.0.8:8000" ||
		backend.TemplateVerdict != conformance.VerdictSafe || backend.PodNamespace != "models" || backend.PodName != "backend-0" {
		t.Fatalf("unexpected registration: %#v", backend)
	}
	if snapshot.Policy.MaxMigrations != v1alpha1.DefaultMaxMigrations || snapshot.Policy.JournalTTL != "10m0s" {
		t.Fatalf("policy was not materialized: %#v", snapshot.Policy)
	}
	route := fixture.getRoute(t)
	ready := meta.FindStatusCondition(route.Status.Conditions, v1alpha1.ConditionReady)
	templateSafe := meta.FindStatusCondition(route.Status.Conditions, v1alpha1.ConditionTemplateSafe)
	degraded := meta.FindStatusCondition(route.Status.Conditions, v1alpha1.ConditionDegraded)
	if ready == nil || ready.Status != metav1.ConditionTrue || templateSafe == nil || templateSafe.Status != metav1.ConditionTrue ||
		degraded == nil || degraded.Status != metav1.ConditionFalse {
		t.Fatalf("conditions = %#v", route.Status.Conditions)
	}
	if route.Status.HealthyBackends != 1 || route.Status.TemplateVerdict != conformance.VerdictSafe ||
		len(route.Status.Backends) != 1 || !route.Status.Backends[0].Ready {
		t.Fatalf("status = %#v", route.Status)
	}

	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if calls := fixture.checker.callCount(); calls != 1 {
		t.Fatalf("stable Ready backend was re-probed: %d calls", calls)
	}
	select {
	case event := <-fixture.events.Events:
		if !strings.Contains(event, "UNKNOWN to SAFE") {
			t.Fatalf("verdict event = %q", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing initial verdict event")
	}
}

func TestInferenceRouteUnsafeBackendServesButIsMigrationIneligible(t *testing.T) {
	t.Parallel()
	fixture := newRouteFixture(t, true, conformance.VerdictUnsafe)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}}
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	snapshot := fixture.admin.lastSnapshot(t)
	if len(snapshot.Backends) != 1 || snapshot.Backends[0].TemplateVerdict != conformance.VerdictUnsafe {
		t.Fatalf("unsafe backend must remain serving with verdict: %#v", snapshot.Backends)
	}
	route := fixture.getRoute(t)
	if route.Status.HealthyBackends != 1 || route.Status.TemplateVerdict != conformance.VerdictUnsafe {
		t.Fatalf("status = %#v", route.Status)
	}
	if condition := meta.FindStatusCondition(route.Status.Conditions, v1alpha1.ConditionTemplateSafe); condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("TemplateSafe condition = %#v", condition)
	}
	if condition := meta.FindStatusCondition(route.Status.Conditions, v1alpha1.ConditionDegraded); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("Degraded condition = %#v", condition)
	}
	select {
	case event := <-fixture.events.Events:
		if !strings.Contains(event, "Warning") || !strings.Contains(event, "UNKNOWN to UNSAFE") {
			t.Fatalf("unsafe event = %q", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing unsafe verdict event")
	}
}

func TestInferenceRouteProbeAndAdminFailuresAreDegradedAndBounded(t *testing.T) {
	t.Parallel()
	t.Run("probe failure is not admitted", func(t *testing.T) {
		fixture := newRouteFixture(t, true, conformance.VerdictSafe)
		fixture.checker.err = errors.New("secret backend response: credential")
		result, err := fixture.reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}})
		if err != nil {
			t.Fatal(err)
		}
		if result.RequeueAfter == 0 || len(fixture.admin.lastSnapshot(t).Backends) != 0 {
			t.Fatalf("result/snapshot = %#v / %#v", result, fixture.admin.lastSnapshot(t))
		}
		route := fixture.getRoute(t)
		if route.Status.Backends[0].Ready || strings.Contains(route.Status.Backends[0].Message, "secret") {
			t.Fatalf("probe error leaked or admitted: %#v", route.Status.Backends[0])
		}
	})
	t.Run("admin failure preserves a conservative degraded status", func(t *testing.T) {
		fixture := newRouteFixture(t, true, conformance.VerdictSafe)
		fixture.admin.err = errors.New("admin unavailable with secret token")
		result, err := fixture.reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}})
		if err != nil {
			t.Fatal(err)
		}
		route := fixture.getRoute(t)
		if result.RequeueAfter == 0 || meta.FindStatusCondition(route.Status.Conditions, v1alpha1.ConditionDegraded).Status != metav1.ConditionTrue {
			t.Fatalf("result/status = %#v / %#v", result, route.Status)
		}
	})
}

func TestInferenceRouteReadyFalseThenTrueReusesExactArtifactVerdict(t *testing.T) {
	t.Parallel()
	fixture := newRouteFixture(t, true, conformance.VerdictSafe)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}}
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	fixture.setEndpointReady(t, false)
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	fixture.setEndpointReady(t, true)
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if fixture.checker.callCount() != 1 {
		t.Fatalf("probe calls = %d, want exact-key cache reuse", fixture.checker.callCount())
	}
	if got := fixture.admin.lastSnapshot(t).Backends[0].TemplateVerdict; got != conformance.VerdictSafe {
		t.Fatalf("cached verdict = %s", got)
	}
}

func TestInferenceRouteRollingReplacementReusesExactArtifactVerdict(t *testing.T) {
	t.Parallel()
	fixture := newRouteFixture(t, true, conformance.VerdictSafe)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}}
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := fixture.client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "backend-0"}, pod); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	pod.ResourceVersion = ""
	pod.Name = "backend-1"
	pod.UID = types.UID("pod-uid-2")
	if err := fixture.client.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	slice := &discoveryv1.EndpointSlice{}
	if err := fixture.client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "vllm-a"}, slice); err != nil {
		t.Fatal(err)
	}
	slice.Endpoints[0].Addresses = []string{"10.0.0.9"}
	slice.Endpoints[0].TargetRef.Name = pod.Name
	slice.Endpoints[0].TargetRef.UID = pod.UID
	if err := fixture.client.Update(context.Background(), slice); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if fixture.checker.callCount() != 1 {
		t.Fatalf("replacement was re-probed despite exact immutable tuple: %d calls", fixture.checker.callCount())
	}
	registration := fixture.admin.lastSnapshot(t).Backends[0]
	if registration.ID != "pod-uid-2" || registration.URL != "http://10.0.0.9:8000" ||
		registration.TemplateVerdict != conformance.VerdictSafe {
		t.Fatalf("replacement registration = %#v", registration)
	}
}

func TestInferenceRouteFinalizerBroadcastsUIDTombstoneBeforeRemoval(t *testing.T) {
	t.Parallel()
	fixture := newRouteFixture(t, true, conformance.VerdictSafe)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "llama"}}
	if _, err := fixture.reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	route := fixture.getRoute(t)
	if _, err := fixture.reconciler.finalize(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	snapshot := fixture.admin.lastSnapshot(t)
	if !snapshot.Deleted || snapshot.UID != "route-uid-1" || snapshot.ObservedGeneration != 1 || len(snapshot.Backends) != 0 {
		t.Fatalf("tombstone = %#v", snapshot)
	}
	route = fixture.getRoute(t)
	if containsString(route.Finalizers, backendSnapshotFinalizer) {
		t.Fatalf("finalizer was not removed: %#v", route.Finalizers)
	}
}

func TestProxyEndpointSliceChangeEnqueuesRoutesAcrossNamespaces(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{discoveryv1.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	routeA := &v1alpha1.InferenceRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "first"}}
	routeB := &v1alpha1.InferenceRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "b", Name: "second"}}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Namespace: "operator", Name: "proxy-a",
		Labels: map[string]string{discoveryv1.LabelServiceName: "proxy"},
	}}
	reconciler := &InferenceRouteReconciler{
		Client:       fake.NewClientBuilder().WithScheme(scheme).WithObjects(routeA, routeB).Build(),
		AdminService: types.NamespacedName{Namespace: "operator", Name: "proxy"},
	}
	requests := reconciler.mapEndpointSliceRoutes(context.Background(), slice)
	found := make(map[types.NamespacedName]bool)
	for _, request := range requests {
		found[request.NamespacedName] = true
	}
	if !found[types.NamespacedName{Namespace: "a", Name: "first"}] ||
		!found[types.NamespacedName{Namespace: "b", Name: "second"}] || len(found) != 2 {
		t.Fatalf("requests = %#v", requests)
	}
}

type routeFixture struct {
	t          *testing.T
	client     client.Client
	reconciler *InferenceRouteReconciler
	checker    *fakeChecker
	admin      *fakeBackendAdmin
	events     *record.FakeRecorder
}

func newRouteFixture(t *testing.T, ready bool, verdict conformance.Verdict) *routeFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	readyValue := ready
	port := int32(8000)
	route := &v1alpha1.InferenceRoute{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "InferenceRoute"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "llama", UID: types.UID("route-uid-1"), Generation: 1},
		Spec: v1alpha1.InferenceRouteSpec{
			Model: "llama", Backends: v1alpha1.BackendPoolSpec{
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "vllm"}}, Port: port,
			}, PolicyRef: corev1.LocalObjectReference{Name: "durable"},
		},
	}
	policy := &v1alpha1.DurabilityPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "DurabilityPolicy"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "durable", Generation: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models", Name: "backend-0", UID: types.UID("pod-uid-1"),
			Labels:      map[string]string{"app": "vllm", labelModelVersion: "sha256:model"},
			Annotations: map[string]string{annotationImageDigest: "sha256:image", annotationTokenizerHash: "sha256:tokenizer"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "vllm:test"}}},
	}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "models", Name: "vllm-a"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: &port}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.8"}, Conditions: discoveryv1.EndpointConditions{Ready: &readyValue},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "models", Name: "backend-0", UID: pod.UID},
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.InferenceRoute{}).
		WithObjects(route, policy, pod, slice).Build()
	checker := &fakeChecker{verdict: verdict, checkedAt: time.Date(2026, 8, 22, 1, 2, 3, 0, time.UTC)}
	admin := &fakeBackendAdmin{}
	events := record.NewFakeRecorder(20)
	reconciler := &InferenceRouteReconciler{
		Client: kubeClient, Checker: checker, Admin: admin, Recorder: events,
		ProbeTimeout: time.Second, ProbeConcurrency: 2,
		Now: func() time.Time { return time.Date(2026, 8, 22, 2, 0, 0, 0, time.UTC) },
	}
	return &routeFixture{t: t, client: kubeClient, reconciler: reconciler, checker: checker, admin: admin, events: events}
}

func (fixture *routeFixture) setEndpointReady(t *testing.T, ready bool) {
	t.Helper()
	slice := &discoveryv1.EndpointSlice{}
	if err := fixture.client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "vllm-a"}, slice); err != nil {
		t.Fatal(err)
	}
	slice.Endpoints[0].Conditions.Ready = &ready
	if err := fixture.client.Update(context.Background(), slice); err != nil {
		t.Fatal(err)
	}
}

func (fixture *routeFixture) getRoute(t *testing.T) *v1alpha1.InferenceRoute {
	t.Helper()
	route := &v1alpha1.InferenceRoute{}
	if err := fixture.client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, route); err != nil {
		t.Fatal(err)
	}
	return route
}

type fakeChecker struct {
	mu        sync.Mutex
	verdict   conformance.Verdict
	err       error
	checkedAt time.Time
	calls     []conformance.CheckRequest
}

func (checker *fakeChecker) Check(_ context.Context, request conformance.CheckRequest) (conformance.Report, error) {
	checker.mu.Lock()
	defer checker.mu.Unlock()
	checker.calls = append(checker.calls, request)
	return conformance.Report{Verdict: checker.verdict, CheckedAt: checker.checkedAt}, checker.err
}

func (checker *fakeChecker) callCount() int {
	checker.mu.Lock()
	defer checker.mu.Unlock()
	return len(checker.calls)
}

type adminCall struct {
	route    types.NamespacedName
	snapshot BackendSnapshot
}

type fakeBackendAdmin struct {
	mu    sync.Mutex
	calls []adminCall
	err   error
}

func (admin *fakeBackendAdmin) Apply(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
	admin.mu.Lock()
	defer admin.mu.Unlock()
	snapshotCopy := snapshot
	snapshotCopy.Backends = append([]BackendRegistration(nil), snapshot.Backends...)
	admin.calls = append(admin.calls, adminCall{route: route, snapshot: snapshotCopy})
	if admin.err != nil {
		return AdminResult{}, admin.err
	}
	return AdminResult{
		Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
		BackendCount: int32(len(snapshot.Backends)),
	}, nil
}

func (admin *fakeBackendAdmin) lastSnapshot(t *testing.T) BackendSnapshot {
	t.Helper()
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.calls) == 0 {
		t.Fatal("admin was not called")
	}
	return admin.calls[len(admin.calls)-1].snapshot
}

func (call adminCall) String() string {
	return fmt.Sprintf("%s generation=%d", call.route, call.snapshot.ObservedGeneration)
}
