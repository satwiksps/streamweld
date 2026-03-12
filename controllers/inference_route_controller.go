package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/streamweld/streamweld/internal/apis/v1alpha1"
	"github.com/streamweld/streamweld/internal/conformance"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	backendSnapshotFinalizer = "streamweld.io/backend-snapshot"
	defaultProbeTimeout      = 45 * time.Second
	defaultProbeConcurrency  = 4
	defaultRetryAfter        = 15 * time.Second
	defaultSuccessfulResync  = 5 * time.Minute
	defaultMaxRouteBackends  = 4096

	annotationImageDigest   = "streamweld.io/image-digest"
	annotationTokenizerHash = "streamweld.io/tokenizer-hash"
	labelModelVersion       = "streamweld.io/model-version"
)

// ConformanceChecker is the shared probe surface used by the controller.
type ConformanceChecker interface {
	Check(context.Context, conformance.CheckRequest) (conformance.Report, error)
}

// InferenceRouteReconciler discovers route backends, probes newly Ready pods,
// and atomically replaces each proxy route snapshot.
type InferenceRouteReconciler struct {
	client.Client
	Checker                 ConformanceChecker
	Admin                   BackendAdmin
	Recorder                record.EventRecorder
	ProbeTimeout            time.Duration
	ProbeConcurrency        int
	MaxBackends             int
	MaxConcurrentReconciles int
	AdminService            types.NamespacedName
	Now                     func() time.Time
}

// +kubebuilder:rbac:groups=streamweld.io,resources=inferenceroutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=streamweld.io,resources=inferenceroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=streamweld.io,resources=inferenceroutes/finalizers,verbs=update
// +kubebuilder:rbac:groups=streamweld.io,resources=durabilitypolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update

// Reconcile applies one complete, generation-fenced route snapshot.
func (reconciler *InferenceRouteReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if reconciler.Client == nil || reconciler.Checker == nil || reconciler.Admin == nil {
		return ctrl.Result{}, errors.New("inference route controller is not fully configured")
	}
	route := &v1alpha1.InferenceRoute{}
	if err := reconciler.Get(ctx, request.NamespacedName, route); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !route.DeletionTimestamp.IsZero() {
		return reconciler.finalize(ctx, route)
	}
	if !containsString(route.Finalizers, backendSnapshotFinalizer) {
		before := route.DeepCopy()
		route.Finalizers = append(route.Finalizers, backendSnapshotFinalizer)
		if err := reconciler.Patch(ctx, route, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, fmt.Errorf("add backend snapshot finalizer: %w", err)
		}
	}

	status := route.Status.DeepCopy()
	status.ObservedGeneration = route.Generation
	now := reconciler.now()
	adminPolicy, policyReady, policyMessage := reconciler.policyForRoute(ctx, route)
	if err := route.Validate(); err != nil {
		policyReady = false
		policyMessage = "route specification is invalid"
	}

	var evaluation routeEvaluation
	if policyReady {
		var err error
		evaluation, err = reconciler.evaluate(ctx, route)
		if err != nil {
			policyReady = false
			policyMessage = publicResolutionMessage(err)
		}
	}
	if !policyReady {
		evaluation = routeEvaluation{
			backends: cloneBackendStatuses(route.Status.Backends),
			verdict:  conformance.VerdictUnknown,
			issues:   []string{policyMessage},
		}
		for index := range evaluation.backends {
			evaluation.backends[index].Ready = false
		}
	}

	snapshot := BackendSnapshot{
		Model: route.Spec.Model, ObservedGeneration: route.Generation, UID: string(route.UID), Deleted: false,
		Policy: adminPolicy, Backends: evaluation.registrations,
	}
	adminResult, adminErr := reconciler.Admin.Apply(ctx, request.NamespacedName, snapshot)
	adminReady := adminErr == nil
	if adminReady {
		status.HealthyBackends = adminResult.BackendCount
		status.DrainingBackends = adminResult.DrainingBackends
		if discoveredDraining := countDraining(evaluation.backends); discoveredDraining > status.DrainingBackends {
			status.DrainingBackends = discoveredDraining
		}
		status.ActiveStreams = adminResult.ActiveStreams
	} else {
		status.HealthyBackends = int32(len(evaluation.registrations))
		status.DrainingBackends = countDraining(evaluation.backends)
		evaluation.issues = append(evaluation.issues, "proxy admin update failed")
	}
	status.Backends = evaluation.backends
	status.TemplateVerdict = evaluation.verdict
	status.TemplateProbedAt = evaluation.probedAt

	ready := adminReady && adminResult.BackendCount > 0
	setCondition(&status.Conditions, now, v1alpha1.ConditionReady, ready,
		conditionReason(ready, "BackendsReady", "NoServingBackends"),
		conditionMessage(ready, "route has serving backends", "route has no admitted serving backend"), route.Generation)
	templateSafe := evaluation.verdict == conformance.VerdictSafe
	setCondition(&status.Conditions, now, v1alpha1.ConditionTemplateSafe, templateSafe,
		templateConditionReason(evaluation.verdict), templateConditionMessage(evaluation.verdict), route.Generation)
	degraded := !policyReady || !adminReady || len(evaluation.issues) != 0 ||
		evaluation.verdict == conformance.VerdictUnsafe || evaluation.verdict == conformance.VerdictDegraded
	setCondition(&status.Conditions, now, v1alpha1.ConditionDegraded, degraded,
		conditionReason(degraded, "ReconciliationDegraded", "ReconciliationHealthy"),
		conditionMessage(degraded, firstIssue(evaluation.issues, evaluation.verdict), "route is fully reconciled"), route.Generation)

	if err := reconciler.updateStatus(ctx, route, *status); err != nil {
		return ctrl.Result{}, err
	}
	reconciler.emitVerdictChanges(route, evaluation.probed)
	if !adminReady || len(evaluation.retryIDs) != 0 || !policyReady {
		return ctrl.Result{RequeueAfter: defaultRetryAfter}, nil
	}
	return ctrl.Result{RequeueAfter: defaultSuccessfulResync}, nil
}

// SetupWithManager registers route, EndpointSlice, Pod, and policy watches.
func (reconciler *InferenceRouteReconciler) SetupWithManager(manager ctrl.Manager) error {
	if reconciler.Client == nil {
		reconciler.Client = manager.GetClient()
	}
	if reconciler.Recorder == nil {
		reconciler.Recorder = manager.GetEventRecorderFor("streamweld-inference-route")
	}
	options := controller.Options{MaxConcurrentReconciles: reconciler.MaxConcurrentReconciles}
	if options.MaxConcurrentReconciles <= 0 {
		options.MaxConcurrentReconciles = 1
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&v1alpha1.InferenceRoute{}).
		Watches(&discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(reconciler.mapEndpointSliceRoutes)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(reconciler.mapNamespaceRoutes)).
		Watches(&v1alpha1.DurabilityPolicy{}, handler.EnqueueRequestsFromMapFunc(reconciler.mapPolicyRoutes)).
		WithOptions(options).
		Complete(reconciler)
}

func (reconciler *InferenceRouteReconciler) mapEndpointSliceRoutes(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	requests := reconciler.mapNamespaceRoutes(ctx, object)
	if reconciler.AdminService.Namespace == "" || reconciler.AdminService.Name == "" ||
		object.GetNamespace() != reconciler.AdminService.Namespace ||
		object.GetLabels()[discoveryv1.LabelServiceName] != reconciler.AdminService.Name {
		return requests
	}
	list := &v1alpha1.InferenceRouteList{}
	if err := reconciler.List(ctx, list); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list routes after proxy EndpointSlice change")
		return requests
	}
	seen := make(map[types.NamespacedName]struct{}, len(requests)+len(list.Items))
	for _, request := range requests {
		seen[request.NamespacedName] = struct{}{}
	}
	for index := range list.Items {
		name := types.NamespacedName{Namespace: list.Items[index].Namespace, Name: list.Items[index].Name}
		if _, exists := seen[name]; exists {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: name})
	}
	return requests
}

type routeEvaluation struct {
	backends      []v1alpha1.BackendStatus
	registrations []BackendRegistration
	probed        []probeChange
	retryIDs      []string
	verdict       conformance.Verdict
	probedAt      *metav1.Time
	issues        []string
}

type resolvedBackend struct {
	id            string
	address       string
	backendURL    string
	modelVersion  string
	podNamespace  string
	podName       string
	imageDigest   string
	tokenizerHash string
	ready         bool
	draining      bool
}

type probeChange struct {
	id       string
	previous conformance.Verdict
	current  conformance.Verdict
}

type probeOutcome struct {
	status v1alpha1.BackendStatus
	change *probeChange
	err    error
}

func (reconciler *InferenceRouteReconciler) evaluate(ctx context.Context, route *v1alpha1.InferenceRoute) (routeEvaluation, error) {
	resolved, resolutionIssues, err := reconciler.resolveBackends(ctx, route)
	if err != nil {
		return routeEvaluation{}, err
	}
	evaluation := routeEvaluation{issues: resolutionIssues, verdict: conformance.VerdictUnknown}
	outcomes := make([]probeOutcome, len(resolved))
	jobs := make(chan int)
	workers := reconciler.probeConcurrency()
	if workers > len(resolved) {
		workers = len(resolved)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				outcomes[index] = reconciler.evaluateBackend(ctx, route, resolved[index])
			}
		}()
	}
	for index := range resolved {
		jobs <- index
	}
	close(jobs)
	wait.Wait()

	for index, outcome := range outcomes {
		evaluation.backends = append(evaluation.backends, outcome.status)
		if outcome.err != nil {
			evaluation.retryIDs = append(evaluation.retryIDs, outcome.status.ID)
			evaluation.issues = append(evaluation.issues, "one or more conformance probes failed")
			continue
		}
		if outcome.change != nil {
			evaluation.probed = append(evaluation.probed, *outcome.change)
		}
		candidate := resolved[index]
		if !outcome.status.Ready || outcome.status.Draining {
			continue
		}
		evaluation.registrations = append(evaluation.registrations, BackendRegistration{
			ID: outcome.status.ID, URL: candidate.backendURL, ModelVersion: candidate.modelVersion,
			TemplateVerdict: outcome.status.TemplateVerdict,
			PodNamespace:    candidate.podNamespace, PodName: candidate.podName,
		})
		evaluation.verdict = leastSafeVerdict(evaluation.verdict, outcome.status.TemplateVerdict)
		if outcome.status.LastProbedAt != nil && (evaluation.probedAt == nil || outcome.status.LastProbedAt.After(evaluation.probedAt.Time)) {
			evaluation.probedAt = outcome.status.LastProbedAt.DeepCopy()
		}
	}
	if len(evaluation.registrations) == 0 {
		evaluation.issues = append(evaluation.issues, "no selected Ready backend has completed conformance admission")
	}
	evaluation.issues = uniqueStrings(evaluation.issues)
	return evaluation, nil
}

func (reconciler *InferenceRouteReconciler) evaluateBackend(
	ctx context.Context,
	route *v1alpha1.InferenceRoute,
	candidate resolvedBackend,
) probeOutcome {
	prior := findBackendStatus(route.Status.Backends, candidate.id)
	status := v1alpha1.BackendStatus{
		ID: candidate.id, Address: candidate.address, Ready: false, Draining: candidate.draining,
		TemplateVerdict: conformance.VerdictUnknown, ImageDigest: candidate.imageDigest, TokenizerHash: candidate.tokenizerHash,
	}
	if prior != nil {
		status.TemplateVerdict = prior.TemplateVerdict
		status.Message = boundedStatusMessage(prior.Message)
		status.LastProbedAt = prior.LastProbedAt.DeepCopy()
	}
	if !candidate.ready || candidate.draining {
		return probeOutcome{status: status}
	}
	if cached, ok := route.FindCachedBackendProbe(route.Spec.Model, candidate.imageDigest, candidate.tokenizerHash); ok &&
		cached.TemplateVerdict != conformance.VerdictUnknown {
		cached.ID = candidate.id
		cached.Address = candidate.address
		cached.Ready = candidate.ready
		cached.Draining = candidate.draining
		cached.ImageDigest = candidate.imageDigest
		cached.TokenizerHash = candidate.tokenizerHash
		return probeOutcome{status: *cached}
	}

	probeContext, cancel := context.WithTimeout(ctx, reconciler.probeTimeout())
	report, err := reconciler.Checker.Check(probeContext, conformance.CheckRequest{
		BackendURL: candidate.backendURL, Model: route.Spec.Model,
		BackendImageDigest: candidate.imageDigest, TokenizerHash: candidate.tokenizerHash,
	})
	cancel()
	if err != nil || report.Verdict == conformance.VerdictUnknown || !report.Verdict.Valid() {
		status.Message = "conformance probe failed; retry is scheduled"
		status.TemplateVerdict = conformance.VerdictUnknown
		status.LastProbedAt = nil
		if err == nil {
			err = errors.New("probe returned no conformance verdict")
		}
		return probeOutcome{status: status, err: err}
	}
	checkedAt := report.CheckedAt.UTC()
	if checkedAt.IsZero() {
		checkedAt = reconciler.now()
	}
	probeTime := metav1.NewTime(checkedAt)
	status.Ready = true
	status.TemplateVerdict = report.Verdict
	status.LastProbedAt = &probeTime
	status.Message = verdictSummary(report.Verdict)
	previous := conformance.VerdictUnknown
	if prior != nil && prior.TemplateVerdict.Valid() {
		previous = prior.TemplateVerdict
	}
	return probeOutcome{
		status: status,
		change: &probeChange{id: candidate.id, previous: previous, current: report.Verdict},
	}
}

func (reconciler *InferenceRouteReconciler) resolveBackends(
	ctx context.Context,
	route *v1alpha1.InferenceRoute,
) ([]resolvedBackend, []string, error) {
	selector, err := metav1.LabelSelectorAsSelector(&route.Spec.Backends.Selector)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve selector: %w", err)
	}
	pods := &corev1.PodList{}
	if err := reconciler.List(ctx, pods, client.InNamespace(route.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, nil, fmt.Errorf("list selected pods: %w", err)
	}
	selectedPods := make(map[types.UID]*corev1.Pod, len(pods.Items))
	podsByName := make(map[string]*corev1.Pod, len(pods.Items))
	for index := range pods.Items {
		pod := &pods.Items[index]
		selectedPods[pod.UID] = pod
		podsByName[pod.Name] = pod
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := reconciler.List(ctx, slices, client.InNamespace(route.Namespace)); err != nil {
		return nil, nil, fmt.Errorf("list EndpointSlices: %w", err)
	}
	sort.Slice(slices.Items, func(left, right int) bool { return slices.Items[left].Name < slices.Items[right].Name })
	candidates := make(map[string]resolvedBackend)
	var issues []string
	for sliceIndex := range slices.Items {
		slice := &slices.Items[sliceIndex]
		if !endpointSliceHasPort(slice, route.Spec.Backends.Port) {
			continue
		}
		for endpointIndex := range slice.Endpoints {
			endpoint := &slice.Endpoints[endpointIndex]
			pod := selectedEndpointPod(route.Namespace, endpoint, selectedPods, podsByName)
			if pod == nil {
				continue
			}
			address, ok := preferredEndpointAddress(slice.AddressType, endpoint.Addresses)
			if !ok {
				issues = append(issues, "one or more selected endpoints has no valid address")
				continue
			}
			id := string(pod.UID)
			if id == "" {
				id = route.Namespace + "/" + pod.Name
			}
			joined := net.JoinHostPort(address, strconv.Itoa(int(route.Spec.Backends.Port)))
			ready := endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready
			draining := endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating
			candidate := resolvedBackend{
				id: id, address: joined, backendURL: "http://" + joined,
				modelVersion: modelVersion(pod), podNamespace: route.Namespace, podName: pod.Name,
				imageDigest: imageDigest(pod), tokenizerHash: tokenizerHash(pod), ready: ready, draining: draining,
			}
			if current, exists := candidates[id]; !exists || preferCandidate(candidate, current) {
				candidates[id] = candidate
			}
		}
	}
	resolved := make([]resolvedBackend, 0, len(candidates))
	for _, candidate := range candidates {
		resolved = append(resolved, candidate)
	}
	sort.Slice(resolved, func(left, right int) bool { return resolved[left].id < resolved[right].id })
	if limit := reconciler.maxBackends(); len(resolved) > limit {
		resolved = resolved[:limit]
		issues = append(issues, fmt.Sprintf("selected backend count exceeds controller limit %d", limit))
	}
	return resolved, uniqueStrings(issues), nil
}

func (reconciler *InferenceRouteReconciler) policyForRoute(
	ctx context.Context,
	route *v1alpha1.InferenceRoute,
) (AdminRoutePolicy, bool, string) {
	fallback := adminPolicyFromSpec(v1alpha1.DefaultDurabilityPolicySpec())
	if route.Spec.PolicyRef.Name == "" {
		return fallback, false, "durability policy reference is missing"
	}
	policy := &v1alpha1.DurabilityPolicy{}
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: route.Namespace, Name: route.Spec.PolicyRef.Name}, policy)
	if apierrors.IsNotFound(err) {
		return fallback, false, "referenced durability policy was not found"
	}
	if err != nil {
		return fallback, false, "referenced durability policy could not be read"
	}
	if err := policy.Validate(); err != nil {
		return fallback, false, "referenced durability policy is invalid"
	}
	return adminPolicyFromSpec(policy.Spec.WithDefaults()), true, ""
}

func (reconciler *InferenceRouteReconciler) finalize(ctx context.Context, route *v1alpha1.InferenceRoute) (ctrl.Result, error) {
	if !containsString(route.Finalizers, backendSnapshotFinalizer) {
		return ctrl.Result{}, nil
	}
	_, err := reconciler.Admin.Apply(ctx, types.NamespacedName{Namespace: route.Namespace, Name: route.Name}, BackendSnapshot{
		Model: route.Spec.Model, ObservedGeneration: route.Generation, UID: string(route.UID), Deleted: true,
		Policy: adminPolicyFromSpec(v1alpha1.DefaultDurabilityPolicySpec()), Backends: []BackendRegistration{},
	})
	if err != nil {
		return ctrl.Result{RequeueAfter: defaultRetryAfter}, nil
	}
	before := route.DeepCopy()
	route.Finalizers = removeString(route.Finalizers, backendSnapshotFinalizer)
	if err := reconciler.Patch(ctx, route, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove backend snapshot finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func adminPolicyFromSpec(spec v1alpha1.DurabilityPolicySpec) AdminRoutePolicy {
	materialized := spec.WithDefaults()
	return AdminRoutePolicy{
		MaxMigrations:         *materialized.MaxMigrations,
		MaxMigrationTokens:    *materialized.MaxMigrationTokens,
		MaxStreamDuration:     materialized.MaxStreamDuration.Duration.String(),
		OrphanPolicy:          string(materialized.OrphanPolicy),
		OrphanTimeout:         materialized.OrphanTimeout.Duration.String(),
		AllowCrossVersion:     materialized.AllowCrossVersion,
		AllowStructuredResume: materialized.AllowStructuredResume,
		SeamWindowBytes:       *materialized.SeamWindowBytes,
		JournalTTL:            materialized.JournalTTL.Duration.String(),
	}
}

func (reconciler *InferenceRouteReconciler) updateStatus(
	ctx context.Context,
	route *v1alpha1.InferenceRoute,
	status v1alpha1.InferenceRouteStatus,
) error {
	if equality.Semantic.DeepEqual(route.Status, status) {
		return nil
	}
	before := route.DeepCopy()
	route.Status = status
	if err := reconciler.Status().Patch(ctx, route, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("update InferenceRoute status: %w", err)
	}
	return nil
}

func (reconciler *InferenceRouteReconciler) emitVerdictChanges(route *v1alpha1.InferenceRoute, changes []probeChange) {
	if reconciler.Recorder == nil {
		return
	}
	for _, change := range changes {
		if change.previous == change.current {
			continue
		}
		eventType := corev1.EventTypeNormal
		if change.current == conformance.VerdictUnsafe {
			eventType = corev1.EventTypeWarning
		}
		reconciler.Recorder.Eventf(route, eventType, "BackendTemplateVerdictChanged",
			"Backend %s template verdict changed from %s to %s", change.id, change.previous, change.current)
	}
}

func (reconciler *InferenceRouteReconciler) mapNamespaceRoutes(ctx context.Context, object client.Object) []reconcile.Request {
	list := &v1alpha1.InferenceRouteList{}
	if err := reconciler.List(ctx, list, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for index := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: list.Items[index].Namespace, Name: list.Items[index].Name,
		}})
	}
	return requests
}

func (reconciler *InferenceRouteReconciler) mapPolicyRoutes(ctx context.Context, object client.Object) []reconcile.Request {
	list := &v1alpha1.InferenceRouteList{}
	if err := reconciler.List(ctx, list, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for index := range list.Items {
		if list.Items[index].Spec.PolicyRef.Name != object.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: list.Items[index].Namespace, Name: list.Items[index].Name,
		}})
	}
	return requests
}

func (reconciler *InferenceRouteReconciler) now() time.Time {
	if reconciler.Now != nil {
		return reconciler.Now().UTC()
	}
	return time.Now().UTC()
}

func (reconciler *InferenceRouteReconciler) probeTimeout() time.Duration {
	if reconciler.ProbeTimeout > 0 {
		return reconciler.ProbeTimeout
	}
	return defaultProbeTimeout
}

func (reconciler *InferenceRouteReconciler) probeConcurrency() int {
	if reconciler.ProbeConcurrency > 0 {
		return reconciler.ProbeConcurrency
	}
	return defaultProbeConcurrency
}

func (reconciler *InferenceRouteReconciler) maxBackends() int {
	if reconciler.MaxBackends > 0 && reconciler.MaxBackends <= maxAdminSnapshotBackends {
		return reconciler.MaxBackends
	}
	return defaultMaxRouteBackends
}

func selectedEndpointPod(
	namespace string,
	endpoint *discoveryv1.Endpoint,
	byUID map[types.UID]*corev1.Pod,
	byName map[string]*corev1.Pod,
) *corev1.Pod {
	if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" ||
		(endpoint.TargetRef.Namespace != "" && endpoint.TargetRef.Namespace != namespace) {
		return nil
	}
	if endpoint.TargetRef.UID != "" {
		if pod := byUID[endpoint.TargetRef.UID]; pod != nil {
			return pod
		}
		return nil
	}
	return byName[endpoint.TargetRef.Name]
}

func endpointSliceHasPort(slice *discoveryv1.EndpointSlice, desired int32) bool {
	for _, port := range slice.Ports {
		if port.Port != nil && *port.Port == desired {
			return true
		}
	}
	return false
}

func preferredEndpointAddress(addressType discoveryv1.AddressType, addresses []string) (string, bool) {
	valid := make([]string, 0, len(addresses))
	for _, address := range addresses {
		switch addressType {
		case discoveryv1.AddressTypeIPv4:
			if parsed := net.ParseIP(address); parsed == nil || parsed.To4() == nil {
				continue
			}
		case discoveryv1.AddressTypeIPv6:
			if parsed := net.ParseIP(address); parsed == nil || parsed.To4() != nil {
				continue
			}
		case discoveryv1.AddressTypeFQDN:
			parsed, err := url.Parse("http://" + address)
			if err != nil || parsed.Hostname() != address || parsed.Port() != "" {
				continue
			}
		default:
			continue
		}
		valid = append(valid, address)
	}
	if len(valid) == 0 {
		return "", false
	}
	sort.Strings(valid)
	return valid[0], true
}

func preferCandidate(candidate, current resolvedBackend) bool {
	if candidate.ready != current.ready {
		return candidate.ready
	}
	if candidate.draining != current.draining {
		return !candidate.draining
	}
	return candidate.address < current.address
}

func imageDigest(pod *corev1.Pod) string {
	if value := strings.TrimSpace(pod.Annotations[annotationImageDigest]); value != "" {
		return boundedIdentity(value)
	}
	statuses := append([]corev1.ContainerStatus(nil), pod.Status.ContainerStatuses...)
	sort.Slice(statuses, func(left, right int) bool { return statuses[left].Name < statuses[right].Name })
	hash := sha256.New()
	found := false
	for _, status := range statuses {
		if value := strings.TrimSpace(status.ImageID); value != "" {
			found = true
			_, _ = hash.Write([]byte(status.Name))
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(value))
			_, _ = hash.Write([]byte{0})
		}
	}
	if found {
		return "sha256:" + hex.EncodeToString(hash.Sum(nil))
	}
	return boundedIdentity("pod-uid:" + string(pod.UID))
}

func tokenizerHash(pod *corev1.Pod) string {
	if value := strings.TrimSpace(pod.Annotations[annotationTokenizerHash]); value != "" {
		return boundedIdentity(value)
	}
	return boundedIdentity("pod-uid:" + string(pod.UID))
}

func modelVersion(pod *corev1.Pod) string {
	if value := strings.TrimSpace(pod.Labels[labelModelVersion]); value != "" {
		return boundedIdentity(value)
	}
	return imageDigest(pod)
}

func boundedIdentity(value string) string {
	const maxIdentityBytes = 256
	value = strings.Map(func(character rune) rune {
		if character < 0x20 || character == 0x7f {
			return -1
		}
		return character
	}, value)
	if len(value) > maxIdentityBytes {
		value = truncateUTF8(value, maxIdentityBytes)
	}
	return value
}

func findBackendStatus(statuses []v1alpha1.BackendStatus, id string) *v1alpha1.BackendStatus {
	for index := range statuses {
		if statuses[index].ID == id {
			return statuses[index].DeepCopy()
		}
	}
	return nil
}

func cloneBackendStatuses(statuses []v1alpha1.BackendStatus) []v1alpha1.BackendStatus {
	cloned := make([]v1alpha1.BackendStatus, len(statuses))
	for index := range statuses {
		cloned[index] = *statuses[index].DeepCopy()
	}
	return cloned
}

func leastSafeVerdict(current, candidate conformance.Verdict) conformance.Verdict {
	rank := map[conformance.Verdict]int{
		conformance.VerdictUnknown:  0,
		conformance.VerdictSafe:     1,
		conformance.VerdictDegraded: 2,
		conformance.VerdictUnsafe:   3,
	}
	if current == conformance.VerdictUnknown || rank[candidate] > rank[current] {
		return candidate
	}
	return current
}

func setCondition(
	conditions *[]metav1.Condition,
	now time.Time,
	typeName string,
	positive bool,
	reason string,
	message string,
	generation int64,
) {
	status := metav1.ConditionFalse
	if positive {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: typeName, Status: status, Reason: reason, Message: boundedStatusMessage(message),
		ObservedGeneration: generation, LastTransitionTime: metav1.NewTime(now),
	})
}

func conditionReason(positive bool, positiveReason, negativeReason string) string {
	if positive {
		return positiveReason
	}
	return negativeReason
}

func conditionMessage(positive bool, positiveMessage, negativeMessage string) string {
	if positive {
		return positiveMessage
	}
	return negativeMessage
}

func templateConditionReason(verdict conformance.Verdict) string {
	switch verdict {
	case conformance.VerdictSafe:
		return "TemplateSafe"
	case conformance.VerdictDegraded:
		return "TemplateDegraded"
	case conformance.VerdictUnsafe:
		return "TemplateUnsafe"
	default:
		return "TemplateUnknown"
	}
}

func templateConditionMessage(verdict conformance.Verdict) string {
	switch verdict {
	case conformance.VerdictSafe:
		return "all admitted backends passed every conformance probe"
	case conformance.VerdictDegraded:
		return "at least one backend has a degraded continuation template"
	case conformance.VerdictUnsafe:
		return "at least one serving backend is migration-ineligible under strict mode"
	default:
		return "no admitted backend has a completed conformance verdict"
	}
}

func verdictSummary(verdict conformance.Verdict) string {
	switch verdict {
	case conformance.VerdictSafe:
		return "all conformance probes passed"
	case conformance.VerdictDegraded:
		return "core continuation probes passed; a secondary probe failed"
	case conformance.VerdictUnsafe:
		return "a required continuation probe failed; serving remains enabled but migration is ineligible"
	default:
		return "conformance verdict is unknown"
	}
}

func firstIssue(issues []string, verdict conformance.Verdict) string {
	if len(issues) != 0 {
		return issues[0]
	}
	return templateConditionMessage(verdict)
}

func boundedStatusMessage(message string) string {
	const maxMessageBytes = 2048
	message = strings.Map(func(character rune) rune {
		if character < 0x20 && character != '\t' {
			return ' '
		}
		return character
	}, message)
	if len(message) > maxMessageBytes {
		message = truncateUTF8(message, maxMessageBytes)
	}
	return message
}

func publicResolutionMessage(error) string { return "backend discovery failed" }

func truncateUTF8(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func countDraining(backends []v1alpha1.BackendStatus) int32 {
	var count int32
	for _, backend := range backends {
		if backend.Draining {
			count++
		}
	}
	return count
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func removeString(values []string, target string) []string {
	result := values[:0]
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}
