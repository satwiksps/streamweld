// Package v1alpha1 contains the Streamweld Kubernetes API.
// +kubebuilder:object:generate=true
// +groupName=streamweld.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the Kubernetes API group owned by Streamweld.
	GroupName = "streamweld.io"
	// Version is the served storage version of the alpha API.
	Version = "v1alpha1"
)

// GroupVersion identifies this API package.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// SchemeBuilder registers every Streamweld API object with a runtime scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds every Streamweld v1alpha1 object to a runtime scheme.
var AddToScheme = SchemeBuilder.AddToScheme

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		GroupVersion,
		&InferenceRoute{},
		&InferenceRouteList{},
		&DurabilityPolicy{},
		&DurabilityPolicyList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// Kind returns a GroupKind in the Streamweld API group.
func Kind(kind string) schema.GroupKind {
	return GroupVersion.WithKind(kind).GroupKind()
}

// Resource returns a GroupResource in the Streamweld API group.
func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}
