package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/streamweld/streamweld/internal/apis/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// PodMutationWebhookPath is the stable admission endpoint used by the chart.
	PodMutationWebhookPath                     = "/mutate-v1-pod"
	defaultTerminationGracePeriodSeconds int64 = 15
)

// WebhookRegistrar is the subset of controller-runtime's webhook server used
// to keep disabled mutation independent of certificates and listeners.
type WebhookRegistrar interface {
	Register(string, http.Handler)
}

// RegisterPodMutationWebhook registers no server or handler when disabled.
func RegisterPodMutationWebhook(registrar WebhookRegistrar, enabled bool, mutator *PodMutator) error {
	if !enabled {
		return nil
	}
	if registrar == nil || mutator == nil {
		return errors.New("pod mutation webhook is enabled but not configured")
	}
	if err := mutator.Validate(); err != nil {
		return err
	}
	registrar.Register(PodMutationWebhookPath, &admission.Webhook{Handler: mutator})
	return nil
}

// PodMutator injects a proxy HTTP preStop drain and a 15-second termination
// grace period into pods selected by a non-deleting InferenceRoute.
type PodMutator struct {
	Client    client.Reader
	DrainHost string
	DrainPort int32
}

// Validate rejects an unsafe or unusable drain endpoint.
func (mutator *PodMutator) Validate() error {
	if mutator == nil || mutator.Client == nil {
		return errors.New("pod mutator Kubernetes client is required")
	}
	host := strings.TrimSpace(mutator.DrainHost)
	if host == "" || host != mutator.DrainHost || strings.ContainsAny(host, "/:@?#\r\n\t ") {
		return errors.New("pod mutator drain host must be an unpadded DNS name or IP address")
	}
	if mutator.DrainPort < 1 || mutator.DrainPort > 65535 {
		return errors.New("pod mutator drain port must be between 1 and 65535")
	}
	return nil
}

// Handle implements admission.Handler.
func (mutator *PodMutator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create {
		return admission.Allowed("pod mutation applies only on create")
	}
	if err := mutator.Validate(); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	var pod corev1.Pod
	if err := json.Unmarshal(request.Object.Raw, &pod); err != nil {
		return admission.Errored(http.StatusBadRequest, errors.New("decode Pod admission request"))
	}
	if pod.Namespace == "" {
		pod.Namespace = request.Namespace
	}
	changed, err := mutator.mutate(ctx, &pod)
	if err != nil {
		return admission.Errored(http.StatusUnprocessableEntity, err)
	}
	if !changed {
		return admission.Allowed("pod is not selected or is already configured")
	}
	mutated, err := json.Marshal(&pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, errors.New("encode mutated Pod"))
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, mutated)
}

func (mutator *PodMutator) mutate(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, errors.New("pod mutation requires a Pod")
	}
	if pod.Namespace == "" || pod.Name == "" {
		return false, errors.New("managed Pod namespace and name are required for drain injection")
	}
	routes := &v1alpha1.InferenceRouteList{}
	if err := mutator.Client.List(ctx, routes, client.InNamespace(pod.Namespace)); err != nil {
		return false, errors.New("list managed inference routes")
	}
	managed := false
	for index := range routes.Items {
		route := &routes.Items[index]
		if !route.DeletionTimestamp.IsZero() {
			continue
		}
		if err := route.Validate(); err != nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&route.Spec.Backends.Selector)
		if err != nil {
			return false, fmt.Errorf("managed route %q has an invalid selector", route.Name)
		}
		if selector.Matches(labelsForPod(pod)) {
			managed = true
			break
		}
	}
	if !managed {
		return false, nil
	}
	if len(pod.Spec.Containers) == 0 {
		return false, errors.New("managed Pod must define at least one container")
	}

	desired := &corev1.LifecycleHandler{HTTPGet: &corev1.HTTPGetAction{
		Path: "/internal/backends/by-pod/" + url.PathEscape(pod.Namespace) + "/" + url.PathEscape(pod.Name) + "/drain",
		Port: intstr.FromInt32(mutator.DrainPort), Host: mutator.DrainHost, Scheme: corev1.URISchemeHTTP,
	}}
	changed := false
	container := &pod.Spec.Containers[0]
	if container.Lifecycle == nil {
		container.Lifecycle = &corev1.Lifecycle{}
		changed = true
	}
	if container.Lifecycle.PreStop == nil {
		container.Lifecycle.PreStop = desired
		changed = true
	} else if !reflect.DeepEqual(container.Lifecycle.PreStop, desired) {
		return false, errors.New("managed Pod's first container already defines a different preStop hook")
	}
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != defaultTerminationGracePeriodSeconds {
		grace := defaultTerminationGracePeriodSeconds
		pod.Spec.TerminationGracePeriodSeconds = &grace
		changed = true
	}
	return changed, nil
}

func labelsForPod(pod *corev1.Pod) labelsSet {
	return labelsSet(pod.Labels)
}

// labelsSet avoids exposing a mutable Pod label map through selector matching.
type labelsSet map[string]string

func (set labelsSet) Has(label string) bool   { _, found := set[label]; return found }
func (set labelsSet) Get(label string) string { return set[label] }
