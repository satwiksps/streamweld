package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/streamweld/streamweld/internal/conformance"
)

const (
	// ConditionReady reports whether a route has at least one serving backend.
	ConditionReady = "Ready"
	// ConditionTemplateSafe reports whether continuation is conformance-safe.
	ConditionTemplateSafe = "TemplateSafe"
	// ConditionDegraded reports a route or policy operating below its intended service level.
	ConditionDegraded = "Degraded"
)

// BackendPoolSpec selects the namespaced backend pods and their inference port.
type BackendPoolSpec struct {
	// Selector selects backend pods and the EndpointSlices belonging to them.
	// +kubebuilder:validation:XValidation:rule="(has(self.matchLabels) && size(self.matchLabels) > 0) || (has(self.matchExpressions) && size(self.matchExpressions) > 0)",message="selector must contain matchLabels or matchExpressions"
	Selector metav1.LabelSelector `json:"selector"`
	// Port is the TCP port serving the OpenAI-compatible backend API.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// InferenceRouteSpec binds one public model name to a backend pool and policy.
type InferenceRouteSpec struct {
	// Model is the exact model identifier sent to and probed on every backend.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern="^[^[:space:]](.*[^[:space:]])?$"
	Model string `json:"model"`
	// Backends selects and addresses the backend pool.
	Backends BackendPoolSpec `json:"backends"`
	// PolicyRef names a DurabilityPolicy in the route's namespace.
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="policyRef.name is required"
	PolicyRef corev1.LocalObjectReference `json:"policyRef"`
}

// BackendStatus is the controller's persisted view of one selected backend.
// Probe cache reuse is safe only through InferenceRoute.FindCachedBackendProbe.
type BackendStatus struct {
	// ID is a stable backend identity, normally derived from the target Pod UID.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	ID string `json:"id"`
	// Address is the currently resolved backend address including its port.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Address string `json:"address"`
	// Ready mirrors endpoint readiness after controller admission.
	Ready bool `json:"ready"`
	// Draining excludes the backend from new streams.
	Draining bool `json:"draining"`
	// TemplateVerdict is the most recent conformance verdict for this cache key.
	// +kubebuilder:validation:Enum=UNKNOWN;SAFE;DEGRADED;UNSAFE
	TemplateVerdict conformance.Verdict `json:"templateVerdict"`
	// Message is a bounded, redacted probe summary. It must never contain
	// credentials, request bodies, or raw model output.
	// +kubebuilder:validation:MaxLength=2048
	Message string `json:"message,omitempty"`
	// LastProbedAt records when TemplateVerdict was produced.
	LastProbedAt *metav1.Time `json:"lastProbedAt,omitempty"`
	// ImageDigest is the immutable backend image component of the probe key.
	// +kubebuilder:validation:MaxLength=256
	ImageDigest string `json:"imageDigest,omitempty"`
	// TokenizerHash is the tokenizer component of the probe key.
	// +kubebuilder:validation:MaxLength=256
	TokenizerHash string `json:"tokenizerHash,omitempty"`
}

// InferenceRouteStatus is the aggregate serving and conformance state.
type InferenceRouteStatus struct {
	// HealthyBackends is the number of ready, admitted, non-draining backends.
	// +kubebuilder:validation:Minimum=0
	HealthyBackends int32 `json:"healthyBackends,omitempty"`
	// DrainingBackends is the number of selected backends currently draining.
	// +kubebuilder:validation:Minimum=0
	DrainingBackends int32 `json:"drainingBackends,omitempty"`
	// TemplateVerdict is the aggregate route verdict.
	// +kubebuilder:validation:Enum=UNKNOWN;SAFE;DEGRADED;UNSAFE
	TemplateVerdict conformance.Verdict `json:"templateVerdict,omitempty"`
	// TemplateProbedAt is the newest successful aggregate probe time.
	TemplateProbedAt *metav1.Time `json:"templateProbedAt,omitempty"`
	// ActiveStreams is the number of non-terminal streams currently using the route.
	// +kubebuilder:validation:Minimum=0
	ActiveStreams int64 `json:"activeStreams,omitempty"`
	// Backends persists per-backend readiness and exact probe-cache metadata.
	// +listType=map
	// +listMapKey=id
	Backends []BackendStatus `json:"backends,omitempty"`
	// ObservedGeneration is the spec generation represented by this status.
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions includes Ready, TemplateSafe, and Degraded.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=inferenceroutes,scope=Namespaced,shortName=ir
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.spec.model`
// +kubebuilder:printcolumn:name="Healthy",type=integer,JSONPath=`.status.healthyBackends`
// +kubebuilder:printcolumn:name="Verdict",type=string,JSONPath=`.status.templateVerdict`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.activeStreams`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// InferenceRoute binds a model to a dynamically discovered backend pool.
type InferenceRoute struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InferenceRouteSpec   `json:"spec"`
	Status InferenceRouteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// InferenceRouteList contains InferenceRoute objects.
type InferenceRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceRoute `json:"items"`
}

// OrphanPolicy controls generation after the final reader detaches.
// +kubebuilder:validation:Enum=continue;cancel_after;cancel
type OrphanPolicy string

const (
	// OrphanContinue leaves the producer running with no attached reader.
	OrphanContinue OrphanPolicy = "continue"
	// OrphanCancelAfter cancels after OrphanTimeout without a reattachment.
	OrphanCancelAfter OrphanPolicy = "cancel_after"
	// OrphanCancel cancels immediately after the final reader detaches.
	OrphanCancel OrphanPolicy = "cancel"
)

// Valid reports whether policy is a defined orphan behavior.
func (policy OrphanPolicy) Valid() bool {
	return policy == OrphanContinue || policy == OrphanCancelAfter || policy == OrphanCancel
}

// DurabilityPolicySpec is the complete per-route migration, resume, orphan,
// and journal-retention policy from the Streamweld protocol.
type DurabilityPolicySpec struct {
	// MaxMigrations is the maximum number of continuation attempts. Zero disables migration.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	MaxMigrations *int32 `json:"maxMigrations,omitempty"`
	// MaxMigrationTokens is the emitted-token eligibility ceiling. Zero disables migration.
	// +kubebuilder:default=8192
	// +kubebuilder:validation:Minimum=0
	MaxMigrationTokens *int64 `json:"maxMigrationTokens,omitempty"`
	// MaxStreamDuration is the positive elapsed-time eligibility ceiling.
	// +kubebuilder:default="15m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="maxStreamDuration must be a positive duration"
	MaxStreamDuration *metav1.Duration `json:"maxStreamDuration,omitempty"`
	// OrphanPolicy controls generation after the final reader detaches.
	// +kubebuilder:default=continue
	OrphanPolicy OrphanPolicy `json:"orphanPolicy,omitempty"`
	// OrphanTimeout is the positive reattachment grace period for cancel_after.
	// +kubebuilder:default="60s"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="orphanTimeout must be a positive duration"
	OrphanTimeout *metav1.Duration `json:"orphanTimeout,omitempty"`
	// AllowCrossVersion permits continuation onto a different model version.
	// +kubebuilder:default=false
	AllowCrossVersion bool `json:"allowCrossVersion,omitempty"`
	// AllowStructuredResume permits validated JSON-prefix continuation.
	// +kubebuilder:default=false
	AllowStructuredResume bool `json:"allowStructuredResume,omitempty"`
	// SeamWindowBytes bounds overlap inspection at a continuation seam.
	// +kubebuilder:default=64
	// +kubebuilder:validation:Minimum=1
	SeamWindowBytes *int32 `json:"seamWindowBytes,omitempty"`
	// JournalTTL is the positive terminal journal and idempotency retention period.
	// +kubebuilder:default="10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="journalTTL must be a positive duration"
	JournalTTL *metav1.Duration `json:"journalTTL,omitempty"`
}

// DurabilityPolicyStatus reports reconciliation health for a policy.
type DurabilityPolicyStatus struct {
	// ObservedGeneration is the spec generation represented by Conditions.
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions includes Ready and Degraded policy reconciliation state.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=durabilitypolicies,scope=Namespaced,shortName=dp
// +kubebuilder:printcolumn:name="Orphan",type=string,JSONPath=`.spec.orphanPolicy`
// +kubebuilder:printcolumn:name="Max Migrations",type=integer,JSONPath=`.spec.maxMigrations`
// +kubebuilder:printcolumn:name="Journal TTL",type=string,JSONPath=`.spec.journalTTL`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DurabilityPolicy defines migration and retention behavior for routes.
type DurabilityPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DurabilityPolicySpec   `json:"spec"`
	Status DurabilityPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DurabilityPolicyList contains DurabilityPolicy objects.
type DurabilityPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DurabilityPolicy `json:"items"`
}
