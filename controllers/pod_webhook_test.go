package controllers

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/satwiksps/streamweld/internal/apis/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestPodMutatorInjectsHTTPDrainAndIsIdempotent(t *testing.T) {
	t.Parallel()
	mutator := newTestPodMutator(t)
	grace := int64(300)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "backend-abc", Labels: map[string]string{"app": "vllm"}},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &grace,
			Containers:                    []corev1.Container{{Name: "backend", Image: "vllm:test"}},
		},
	}
	changed, err := mutator.mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != 15 {
		t.Fatalf("grace mutation = %v / %v", changed, pod.Spec.TerminationGracePeriodSeconds)
	}
	hook := pod.Spec.Containers[0].Lifecycle.PreStop
	if hook == nil || hook.HTTPGet == nil {
		t.Fatalf("missing HTTP preStop: %#v", pod.Spec.Containers[0].Lifecycle)
	}
	if hook.HTTPGet.Host != "streamweld-operator.operator.svc" || hook.HTTPGet.Port.IntVal != 8082 ||
		hook.HTTPGet.Path != "/internal/backends/by-pod/models/backend-abc/drain" || hook.HTTPGet.Scheme != corev1.URISchemeHTTP {
		t.Fatalf("HTTP preStop = %#v", hook.HTTPGet)
	}
	changed, err = mutator.mutate(context.Background(), pod)
	if err != nil || changed {
		t.Fatalf("second mutation changed=%v err=%v", changed, err)
	}
}

func TestPodMutatorOptOutDoesNotRegisterWebhookServer(t *testing.T) {
	t.Parallel()
	if err := RegisterPodMutationWebhook(nil, false, nil); err != nil {
		t.Fatalf("disabled registration = %v", err)
	}
	registrar := &fakeWebhookRegistrar{}
	if err := RegisterPodMutationWebhook(registrar, true, newTestPodMutator(t)); err != nil {
		t.Fatal(err)
	}
	if registrar.calls != 1 || registrar.path != PodMutationWebhookPath || registrar.handler == nil {
		t.Fatalf("registration = %#v", registrar)
	}
}

func TestPodMutatorLeavesUnmanagedPodsAndRejectsHookOverwrite(t *testing.T) {
	t.Parallel()
	mutator := newTestPodMutator(t)
	unmanaged := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "database", Labels: map[string]string{"app": "postgres"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "database"}}},
	}
	if changed, err := mutator.mutate(context.Background(), unmanaged); err != nil || changed {
		t.Fatalf("unmanaged mutation changed=%v err=%v", changed, err)
	}
	managed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "backend", Labels: map[string]string{"app": "vllm"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "backend", Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{Command: []string{"/bin/cleanup"}},
			}},
		}}},
	}
	if changed, err := mutator.mutate(context.Background(), managed); err == nil || changed {
		t.Fatalf("conflicting hook changed=%v err=%v", changed, err)
	}
}

func TestPodMutatorAdmissionResponseContainsPatch(t *testing.T) {
	t.Parallel()
	mutator := newTestPodMutator(t)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "backend", Labels: map[string]string{"app": "vllm"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "backend"}}},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	response := mutator.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create, Namespace: "models", Object: runtime.RawExtension{Raw: raw},
	}})
	if !response.Allowed || len(response.Patches) == 0 {
		t.Fatalf("admission response = %#v", response)
	}
}

func newTestPodMutator(t *testing.T) *PodMutator {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	route := &v1alpha1.InferenceRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "llama"},
		Spec: v1alpha1.InferenceRouteSpec{
			Model: "llama", PolicyRef: corev1.LocalObjectReference{Name: "durable"},
			Backends: v1alpha1.BackendPoolSpec{
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "vllm"}}, Port: 8000,
			},
		},
	}
	return &PodMutator{
		Client:    fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build(),
		DrainHost: "streamweld-operator.operator.svc", DrainPort: 8082,
	}
}

type fakeWebhookRegistrar struct {
	calls   int
	path    string
	handler http.Handler
}

func (registrar *fakeWebhookRegistrar) Register(path string, handler http.Handler) {
	registrar.calls++
	registrar.path = path
	registrar.handler = handler
}
