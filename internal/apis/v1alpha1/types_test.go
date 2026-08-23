package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/satwiksps/streamweld/internal/conformance"
)

func TestAddToSchemeRegistersAllRootKinds(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	for _, object := range []runtime.Object{
		&InferenceRoute{}, &InferenceRouteList{}, &DurabilityPolicy{}, &DurabilityPolicyList{},
	} {
		kinds, unversioned, err := scheme.ObjectKinds(object)
		if err != nil || unversioned || len(kinds) != 1 || kinds[0].GroupVersion() != GroupVersion {
			t.Errorf("ObjectKinds(%T) = (%v, %t, %v)", object, kinds, unversioned, err)
		}
	}
	if got := Kind("InferenceRoute").String(); got != "InferenceRoute.streamweld.io" {
		t.Errorf("Kind() = %q", got)
	}
	if got := Resource("inferenceroutes").String(); got != "inferenceroutes.streamweld.io" {
		t.Errorf("Resource() = %q", got)
	}
}

func TestInferenceRouteValidation(t *testing.T) {
	t.Parallel()
	valid := validInferenceRoute()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid route rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*InferenceRoute)
		want   string
	}{
		{"blank model", func(route *InferenceRoute) { route.Spec.Model = "  " }, "spec.model"},
		{"padded model", func(route *InferenceRoute) { route.Spec.Model = " model" }, "surrounding whitespace"},
		{"empty selector", func(route *InferenceRoute) { route.Spec.Backends.Selector = metav1.LabelSelector{} }, "spec.backends.selector"},
		{"invalid selector", func(route *InferenceRoute) {
			route.Spec.Backends.Selector = metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: "app", Operator: metav1.LabelSelectorOperator("Near"), Values: []string{"vllm"},
			}}}
		}, "valid label selector operator"},
		{"zero port", func(route *InferenceRoute) { route.Spec.Backends.Port = 0 }, "spec.backends.port"},
		{"large port", func(route *InferenceRoute) { route.Spec.Backends.Port = 65536 }, "between 1 and 65535"},
		{"empty policy", func(route *InferenceRoute) { route.Spec.PolicyRef.Name = "" }, "spec.policyRef.name"},
		{"invalid policy name", func(route *InferenceRoute) { route.Spec.PolicyRef.Name = "Not_Valid" }, "lower case"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route := valid.DeepCopy()
			test.mutate(route)
			err := route.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
	var nilRoute *InferenceRoute
	if err := nilRoute.Validate(); err == nil {
		t.Fatal("nil InferenceRoute passed validation")
	}
}

func TestDurabilityPolicyDefaultsPreserveExplicitZeroLimits(t *testing.T) {
	t.Parallel()
	zeroMigrations := int32(0)
	zeroTokens := int64(0)
	spec := DurabilityPolicySpec{
		MaxMigrations:         &zeroMigrations,
		MaxMigrationTokens:    &zeroTokens,
		OrphanPolicy:          OrphanCancel,
		AllowCrossVersion:     true,
		AllowStructuredResume: true,
	}
	defaulted := spec.WithDefaults()
	if spec.MaxStreamDuration != nil || spec.OrphanTimeout != nil || spec.SeamWindowBytes != nil || spec.JournalTTL != nil {
		t.Fatal("WithDefaults() mutated its receiver")
	}
	if *defaulted.MaxMigrations != 0 || *defaulted.MaxMigrationTokens != 0 {
		t.Fatalf("explicit disabling limits were overwritten: %+v", defaulted)
	}
	if defaulted.MaxStreamDuration.Duration != DefaultMaxStreamDuration ||
		defaulted.OrphanPolicy != OrphanCancel ||
		defaulted.OrphanTimeout.Duration != DefaultOrphanTimeout ||
		*defaulted.SeamWindowBytes != DefaultSeamWindowBytes ||
		defaulted.JournalTTL.Duration != DefaultJournalTTL ||
		!defaulted.AllowCrossVersion || !defaulted.AllowStructuredResume {
		t.Fatalf("defaulted spec = %+v", defaulted)
	}

	defaults := DefaultDurabilityPolicySpec()
	if *defaults.MaxMigrations != DefaultMaxMigrations ||
		*defaults.MaxMigrationTokens != DefaultMaxMigrationTokens ||
		defaults.MaxStreamDuration.Duration != DefaultMaxStreamDuration ||
		defaults.OrphanPolicy != DefaultOrphanPolicy ||
		defaults.OrphanTimeout.Duration != DefaultOrphanTimeout ||
		defaults.AllowCrossVersion || defaults.AllowStructuredResume ||
		*defaults.SeamWindowBytes != DefaultSeamWindowBytes ||
		defaults.JournalTTL.Duration != DefaultJournalTTL {
		t.Fatalf("DefaultDurabilityPolicySpec() = %+v", defaults)
	}
	var nilSpec *DurabilityPolicySpec
	nilDefaults := nilSpec.WithDefaults()
	if *nilDefaults.MaxMigrations != DefaultMaxMigrations || nilDefaults.JournalTTL.Duration != DefaultJournalTTL {
		t.Fatalf("nil WithDefaults() = %+v", nilDefaults)
	}
	policy := &DurabilityPolicy{}
	policy.ApplyDefaults()
	if policy.Spec.MaxMigrations == nil || policy.Spec.JournalTTL == nil {
		t.Fatalf("DurabilityPolicy.ApplyDefaults() = %+v", policy.Spec)
	}
}

func TestDurabilityPolicyValidation(t *testing.T) {
	t.Parallel()
	if err := (&DurabilityPolicy{}).Validate(); err != nil {
		t.Fatalf("defaultable empty policy rejected: %v", err)
	}
	zeroMigrations := int32(0)
	zeroTokens := int64(0)
	if err := (&DurabilityPolicy{Spec: DurabilityPolicySpec{
		MaxMigrations: &zeroMigrations, MaxMigrationTokens: &zeroTokens,
	}}).Validate(); err != nil {
		t.Fatalf("zero migration limits rejected: %v", err)
	}

	negativeMigrations := int32(-1)
	negativeTokens := int64(-1)
	zeroSeam := int32(0)
	policy := &DurabilityPolicy{Spec: DurabilityPolicySpec{
		MaxMigrations:      &negativeMigrations,
		MaxMigrationTokens: &negativeTokens,
		MaxStreamDuration:  &metav1.Duration{},
		OrphanPolicy:       "later",
		OrphanTimeout:      &metav1.Duration{Duration: -time.Second},
		SeamWindowBytes:    &zeroSeam,
		JournalTTL:         &metav1.Duration{},
	}}
	err := policy.Validate()
	if err == nil {
		t.Fatal("invalid policy passed validation")
	}
	for _, fieldName := range []string{
		"maxMigrations", "maxMigrationTokens", "maxStreamDuration", "orphanPolicy",
		"orphanTimeout", "seamWindowBytes", "journalTTL",
	} {
		if !strings.Contains(err.Error(), fieldName) {
			t.Errorf("validation error %q does not include %s", err, fieldName)
		}
	}
	var nilPolicy *DurabilityPolicy
	if err := nilPolicy.Validate(); err == nil {
		t.Fatal("nil DurabilityPolicy passed validation")
	}
}

func TestDeepCopyIsIndependent(t *testing.T) {
	t.Parallel()
	route := validInferenceRoute()
	clone := route.DeepCopy()
	clone.Labels["team"] = "changed"
	clone.Spec.Backends.Selector.MatchLabels["app"] = "changed"
	clone.Spec.Backends.Selector.MatchExpressions[0].Values[0] = "changed"
	clone.Status.Backends[0].Message = "changed"
	clone.Status.Backends[0].LastProbedAt.Time = clone.Status.Backends[0].LastProbedAt.Add(time.Hour)
	clone.Status.Conditions[0].Message = "changed"
	if route.Labels["team"] != "inference" ||
		route.Spec.Backends.Selector.MatchLabels["app"] != "vllm" ||
		route.Spec.Backends.Selector.MatchExpressions[0].Values[0] != "gpu" ||
		route.Status.Backends[0].Message != "all probes passed" ||
		route.Status.Conditions[0].Message != "three backends ready" ||
		route.Status.Backends[0].LastProbedAt.Equal(clone.Status.Backends[0].LastProbedAt) {
		t.Fatalf("InferenceRoute DeepCopy shared mutable state: original=%+v clone=%+v", route, clone)
	}

	policy := &DurabilityPolicy{Spec: DefaultDurabilityPolicySpec(), Status: DurabilityPolicyStatus{
		Conditions: []metav1.Condition{{Type: ConditionReady, Message: "valid"}},
	}}
	policyCopy := policy.DeepCopy()
	*policyCopy.Spec.MaxMigrations = 99
	policyCopy.Spec.MaxStreamDuration.Duration = time.Second
	policyCopy.Status.Conditions[0].Message = "changed"
	if *policy.Spec.MaxMigrations != DefaultMaxMigrations ||
		policy.Spec.MaxStreamDuration.Duration != DefaultMaxStreamDuration ||
		policy.Status.Conditions[0].Message != "valid" {
		t.Fatalf("DurabilityPolicy DeepCopy shared mutable state: original=%+v copy=%+v", policy, policyCopy)
	}

	list := (&InferenceRouteList{Items: []InferenceRoute{*route}}).DeepCopy()
	list.Items[0].Spec.Model = "changed"
	if route.Spec.Model == "changed" {
		t.Fatal("InferenceRouteList DeepCopy shared an item")
	}
}

func TestSerializationUsesStableAPIFieldsAndTypedDurations(t *testing.T) {
	t.Parallel()
	policy := &DurabilityPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "DurabilityPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: "default-durable", Namespace: "models"},
		Spec:       DefaultDurabilityPolicySpec(),
	}
	encoded, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, want := range []string{
		`"apiVersion":"streamweld.io/v1alpha1"`, `"maxMigrations":3`,
		`"maxMigrationTokens":8192`, `"maxStreamDuration":"15m0s"`,
		`"orphanPolicy":"continue"`, `"journalTTL":"10m0s"`,
	} {
		if !strings.Contains(string(encoded), want) {
			t.Errorf("serialized policy %s does not contain %s", encoded, want)
		}
	}

	var decoded DurabilityPolicy
	if err := json.Unmarshal([]byte(`{
		"apiVersion":"streamweld.io/v1alpha1","kind":"DurabilityPolicy",
		"metadata":{"name":"disabled"},"spec":{
			"maxMigrations":0,"maxMigrationTokens":0,"maxStreamDuration":"1h30m",
			"orphanPolicy":"cancel_after","orphanTimeout":"45s",
			"allowCrossVersion":true,"allowStructuredResume":true,
			"seamWindowBytes":128,"journalTTL":"20m"
		}}`), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded.Spec.MaxMigrations == nil || *decoded.Spec.MaxMigrations != 0 ||
		decoded.Spec.MaxStreamDuration.Duration != 90*time.Minute ||
		decoded.Spec.OrphanPolicy != OrphanCancelAfter || !decoded.Spec.AllowCrossVersion {
		t.Fatalf("decoded typed policy = %+v", decoded.Spec)
	}

	routeJSON, err := json.Marshal(validInferenceRoute())
	if err != nil {
		t.Fatalf("marshal route: %v", err)
	}
	for _, want := range []string{
		`"policyRef":{"name":"default-durable"}`, `"healthyBackends":3`,
		`"templateVerdict":"SAFE"`, `"imageDigest":"sha256:image"`,
		`"tokenizerHash":"sha256:tokenizer"`,
	} {
		if !strings.Contains(string(routeJSON), want) {
			t.Errorf("serialized route %s does not contain %s", routeJSON, want)
		}
	}
}

func TestFindCachedBackendProbeRequiresExactCacheKeyAndGeneration(t *testing.T) {
	t.Parallel()
	route := validInferenceRoute()
	cached, ok := route.FindCachedBackendProbe(
		route.Spec.Model, "sha256:image", "sha256:tokenizer",
	)
	if !ok || cached.TemplateVerdict != conformance.VerdictSafe {
		t.Fatalf("exact cache lookup = (%+v, %t)", cached, ok)
	}
	if cached.ID != "" || cached.Address != "" || cached.Ready || cached.Draining {
		t.Fatalf("cache lookup leaked backend identity or serving state: %+v", cached)
	}
	cached.Message = "caller mutation"
	if route.Status.Backends[0].Message == cached.Message {
		t.Fatal("cache lookup returned mutable status storage")
	}
	for _, test := range []struct {
		name      string
		model     string
		digest    string
		tokenizer string
		mutate    func(*InferenceRoute)
	}{
		{"wrong model", "other-model", "sha256:image", "sha256:tokenizer", nil},
		{"wrong digest", route.Spec.Model, "sha256:other", "sha256:tokenizer", nil},
		{"wrong tokenizer", route.Spec.Model, "sha256:image", "sha256:other", nil},
		{"stale generation", route.Spec.Model, "sha256:image", "sha256:tokenizer", func(clone *InferenceRoute) {
			clone.Status.ObservedGeneration--
		}},
		{"missing probe time", route.Spec.Model, "sha256:image", "sha256:tokenizer", func(clone *InferenceRoute) {
			clone.Status.Backends[0].LastProbedAt = nil
		}},
		{"invalid verdict", route.Spec.Model, "sha256:image", "sha256:tokenizer", func(clone *InferenceRoute) {
			clone.Status.Backends[0].TemplateVerdict = "BROKEN"
		}},
		{"unknown verdict", route.Spec.Model, "sha256:image", "sha256:tokenizer", func(clone *InferenceRoute) {
			clone.Status.Backends[0].TemplateVerdict = conformance.VerdictUnknown
		}},
		{"empty model", "", "sha256:image", "sha256:tokenizer", nil},
		{"empty digest", route.Spec.Model, "", "sha256:tokenizer", nil},
		{"empty tokenizer", route.Spec.Model, "sha256:image", "", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			clone := route.DeepCopy()
			if test.mutate != nil {
				test.mutate(clone)
			}
			if cached, ok := clone.FindCachedBackendProbe(test.model, test.digest, test.tokenizer); ok || cached != nil {
				t.Fatalf("mismatched cache lookup = (%+v, %t)", cached, ok)
			}
		})
	}
}

func TestFindCachedBackendProbeReusesAcrossBackendIdentityAndChoosesNewest(t *testing.T) {
	t.Parallel()
	route := validInferenceRoute()
	newer := route.Status.Backends[0].DeepCopy()
	newer.ID = "replacement-pod-uid"
	newer.Address = "10.0.0.2:8000"
	newer.Ready = false
	newer.Draining = true
	newer.TemplateVerdict = conformance.VerdictDegraded
	newer.Message = "newest exact-key verdict"
	newerTime := metav1.NewTime(newer.LastProbedAt.Add(time.Minute))
	newer.LastProbedAt = &newerTime
	route.Status.Backends = append(route.Status.Backends, *newer)

	cached, ok := route.FindCachedBackendProbe(route.Spec.Model, "sha256:image", "sha256:tokenizer")
	if !ok || cached.TemplateVerdict != conformance.VerdictDegraded || cached.Message != newer.Message ||
		cached.LastProbedAt == nil || !cached.LastProbedAt.Equal(&newerTime) {
		t.Fatalf("newest cross-backend cache lookup = (%+v, %t)", cached, ok)
	}
	if cached.ID != "" || cached.Address != "" || cached.Ready || cached.Draining {
		t.Fatalf("cross-backend cache lookup retained transient fields: %+v", cached)
	}
}

func validInferenceRoute() *InferenceRoute {
	probedAt := metav1.NewTime(time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC))
	return &InferenceRoute{
		TypeMeta: metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "InferenceRoute"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "llama-8b", Namespace: "models", Generation: 4,
			Labels: map[string]string{"team": "inference"},
		},
		Spec: InferenceRouteSpec{
			Model: "meta-llama/Llama-3.1-8B-Instruct",
			Backends: BackendPoolSpec{
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "vllm"},
					MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key: "accelerator", Operator: metav1.LabelSelectorOpIn, Values: []string{"gpu"},
					}},
				},
				Port: 8000,
			},
			PolicyRef: corev1.LocalObjectReference{Name: "default-durable"},
		},
		Status: InferenceRouteStatus{
			HealthyBackends: 3, DrainingBackends: 1,
			TemplateVerdict: conformance.VerdictSafe, TemplateProbedAt: &probedAt,
			ActiveStreams: 12, ObservedGeneration: 4,
			Backends: []BackendStatus{{
				ID: "pod-uid-a", Address: "10.0.0.1:8000", Ready: true,
				TemplateVerdict: conformance.VerdictSafe, Message: "all probes passed",
				LastProbedAt: &probedAt, ImageDigest: "sha256:image", TokenizerHash: "sha256:tokenizer",
			}},
			Conditions: []metav1.Condition{{
				Type: ConditionReady, Status: metav1.ConditionTrue, Reason: "BackendsReady",
				Message: "three backends ready", LastTransitionTime: probedAt,
			}},
		},
	}
}
