package controllers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEndpointFanoutAdminUpdatesEveryReadyProxyAndAggregatesCounts(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	var mutex sync.Mutex
	called := make(map[string]int)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		mutex.Lock()
		called[endpoint]++
		mutex.Unlock()
		active, draining := int64(2), int32(1)
		drainingIDs := []string{"retired/pod-a"}
		if strings.Contains(endpoint, "10.0.0.2") {
			active = 3
			draining = 2
			drainingIDs = []string{"retired/pod-b", "retired/pod-c"}
		}
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			return AdminResult{
				Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
				BackendCount: 4, DrainingBackends: draining, DrainingBackendIDs: drainingIDs, ActiveStreams: active,
			}, nil
		}), nil
	}
	result, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BackendCount != 4 || result.DrainingBackends != 3 || result.ActiveStreams != 5 ||
		!slices.Equal(result.DrainingBackendIDs, []string{"retired/pod-a", "retired/pod-b", "retired/pod-c"}) {
		t.Fatalf("aggregate = %#v", result)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(called) != 2 || called["http://10.0.0.1:8080"] != 1 || called["http://10.0.0.2:8080"] != 1 {
		t.Fatalf("called endpoints = %#v", called)
	}
}

func TestEndpointFanoutAdminDeduplicatesDrainingBackendsAcrossReplicas(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		ids := []string{"retired/pod-a", "retired/shared"}
		if strings.Contains(endpoint, "10.0.0.2") {
			ids = []string{"retired/pod-b", "retired/shared"}
		}
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			return AdminResult{
				Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
				BackendCount: 4, DrainingBackends: 2, DrainingBackendIDs: ids,
			}, nil
		}), nil
	}
	result, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"retired/pod-a", "retired/pod-b", "retired/shared"}
	if result.DrainingBackends != 3 || !slices.Equal(result.DrainingBackendIDs, want) {
		t.Fatalf("aggregate = %#v, want deduplicated IDs %v", result, want)
	}
}

func TestEndpointFanoutAdminUsesMaxLegacyDrainingCount(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		draining := int32(1)
		if strings.Contains(endpoint, "10.0.0.2") {
			draining = 3
		}
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			return AdminResult{
				Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
				BackendCount: 4, DrainingBackends: draining,
			}, nil
		}), nil
	}
	result, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.DrainingBackends != 3 || len(result.DrainingBackendIDs) != 0 {
		t.Fatalf("legacy aggregate = %#v", result)
	}
}

func TestEndpointFanoutAdminDoesNotClaimCompleteIDsWithLegacyOverlap(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		result := AdminResult{BackendCount: 4, DrainingBackends: 2}
		if strings.Contains(endpoint, "10.0.0.1") {
			result.DrainingBackendIDs = []string{"retired/a", "retired/b"}
		}
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			result.Route = route.String()
			result.Model = snapshot.Model
			result.AppliedGeneration = snapshot.ObservedGeneration
			return result, nil
		}), nil
	}
	result, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.DrainingBackends != 2 || len(result.DrainingBackendIDs) != 0 {
		t.Fatalf("mixed-version aggregate claimed complete identities: %#v", result)
	}
}

func TestEndpointFanoutAdminStillRejectsServingBackendMismatch(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		serving := int32(4)
		if strings.Contains(endpoint, "10.0.0.2") {
			serving = 5
		}
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			return AdminResult{
				Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
				BackendCount: serving,
			}, nil
		}), nil
	}
	_, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err == nil || !strings.Contains(err.Error(), "1 of 2") {
		t.Fatalf("error = %v", err)
	}
}

func TestEndpointFanoutAdminRejectsAppliedGenerationMismatch(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		generation := int64(3)
		if strings.Contains(endpoint, "10.0.0.2") {
			generation = 4
		}
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			return AdminResult{
				Route: route.String(), Model: snapshot.Model, AppliedGeneration: generation,
				BackendCount: 4,
			}, nil
		}), nil
	}
	_, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err == nil || !strings.Contains(err.Error(), "1 of 2") {
		t.Fatalf("error = %v", err)
	}
}

func TestEndpointFanoutAdminRequiresEveryReadyProxyAcknowledgement(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			if strings.Contains(endpoint, "10.0.0.2") {
				return AdminResult{}, errors.New("replica unavailable")
			}
			return AdminResult{Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration}, nil
		}), nil
	}
	_, err := admin.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 3, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err == nil || !strings.Contains(err.Error(), "1 of 2") || strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("error = %v", err)
	}
}

func TestEndpointFanoutAdminRejectsNoReadyProxy(t *testing.T) {
	t.Parallel()
	// Build a second fanout with an empty fake client; the production behavior
	// must be a conservative retry rather than a ClusterIP fallback.
	scheme := runtime.NewScheme()
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	empty, err := NewEndpointFanoutAdmin(EndpointFanoutAdminConfig{
		Reader:  fake.NewClientBuilder().WithScheme(scheme).Build(),
		Service: types.NamespacedName{Namespace: "operator", Name: "streamweld-proxy"}, EndpointPort: 8080,
		Client: AdminClientConfig{BaseURL: "http://streamweld-proxy.operator.svc:8080", BearerToken: "token", AllowInsecureHTTP: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = empty.Apply(context.Background(), types.NamespacedName{Namespace: "models", Name: "llama"}, BackendSnapshot{
		Model: "llama", ObservedGeneration: 1, UID: "route-uid", Policy: testAdminPolicy(),
	})
	if err == nil || !strings.Contains(err.Error(), "no Ready endpoints") {
		t.Fatalf("error = %v", err)
	}
}

func TestEndpointFanoutAdminTombstonesNotReadyReplicaBeforeRouteRecreation(t *testing.T) {
	t.Parallel()
	admin := newTestFanoutAdmin(t)
	var mutex sync.Mutex
	state := map[string]string{
		"http://10.0.0.1:8080": "live:old-uid",
		"http://10.0.0.2:8080": "live:old-uid",
		"http://10.0.0.3:8080": "live:old-uid",
	}
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		return backendAdminFunc(func(_ context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
			mutex.Lock()
			defer mutex.Unlock()
			if snapshot.Deleted {
				state[endpoint] = "tombstone:" + snapshot.UID
			} else {
				if current := state[endpoint]; strings.HasPrefix(current, "live:") && current != "live:"+snapshot.UID {
					return AdminResult{}, errors.New("cross-UID live replacement")
				}
				state[endpoint] = "live:" + snapshot.UID
			}
			return AdminResult{Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration}, nil
		}), nil
	}
	route := types.NamespacedName{Namespace: "models", Name: "llama"}
	if _, err := admin.Apply(context.Background(), route, BackendSnapshot{
		Model: "llama", ObservedGeneration: 4, UID: "old-uid", Deleted: true, Policy: testAdminPolicy(),
	}); err != nil {
		t.Fatal(err)
	}
	mutex.Lock()
	if state["http://10.0.0.3:8080"] != "tombstone:old-uid" {
		t.Fatalf("NotReady replica missed tombstone: %#v", state)
	}
	mutex.Unlock()

	kubeClient, ok := admin.reader.(client.Client)
	if !ok {
		t.Fatal("test fanout reader cannot update EndpointSlices")
	}
	slice := &discoveryv1.EndpointSlice{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: "operator", Name: "proxy-a"}, slice); err != nil {
		t.Fatal(err)
	}
	ready := true
	slice.Endpoints[2].Conditions.Ready = &ready
	if err := kubeClient.Update(context.Background(), slice); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Apply(context.Background(), route, BackendSnapshot{
		Model: "llama", ObservedGeneration: 1, UID: "new-uid", Policy: testAdminPolicy(),
	}); err != nil {
		t.Fatalf("recreated route could not reach every replica: %v", err)
	}
}

func newTestFanoutAdmin(t *testing.T) *EndpointFanoutAdmin {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ready := true
	notReady := false
	port := int32(8080)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "operator", Name: "proxy-a",
			Labels: map[string]string{discoveryv1.LabelServiceName: "streamweld-proxy"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: &port}},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: "proxy-1"}},
			{Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: "proxy-2"}},
			{Addresses: []string{"10.0.0.3"}, Conditions: discoveryv1.EndpointConditions{Ready: &notReady}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: "proxy-3"}},
		},
	}
	dualStack := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "operator", Name: "proxy-v6",
			Labels: map[string]string{discoveryv1.LabelServiceName: "streamweld-proxy"},
		},
		AddressType: discoveryv1.AddressTypeIPv6,
		Ports:       []discoveryv1.EndpointPort{{Port: &port}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"2001:db8::1"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: "proxy-1"},
		}},
	}
	admin, err := NewEndpointFanoutAdmin(EndpointFanoutAdminConfig{
		Reader:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(slice, dualStack).Build(),
		Service: types.NamespacedName{Namespace: "operator", Name: "streamweld-proxy"}, EndpointPort: port,
		Client:      AdminClientConfig{BaseURL: "http://streamweld-proxy.operator.svc:8080", BearerToken: "token", AllowInsecureHTTP: true},
		Concurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return admin
}

type backendAdminFunc func(context.Context, types.NamespacedName, BackendSnapshot) (AdminResult, error)

func (function backendAdminFunc) Apply(ctx context.Context, route types.NamespacedName, snapshot BackendSnapshot) (AdminResult, error) {
	return function(ctx, route, snapshot)
}
