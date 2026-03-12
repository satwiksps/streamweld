package proxy

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/conformance"
)

const (
	maxRouteBackends                 = 4096
	maxRouteResultDrainingBackendIDs = 32
)

var (
	errRouteGenerationStale = errors.New("proxy: route generation is stale")
	errRouteUIDConflict     = errors.New("proxy: route UID conflicts with the live object")
	routeNamespacePattern   = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)
	routeNamePattern        = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$`)
)

type routeBackendUpdate struct {
	Model              string              `json:"model"`
	UID                string              `json:"uid"`
	ObservedGeneration int64               `json:"observed_generation"`
	Deleted            bool                `json:"deleted"`
	Policy             routePolicyInput    `json:"policy"`
	Backends           []routeBackendInput `json:"backends"`
}

type routePolicyInput struct {
	MaxMigrations         int          `json:"max_migrations"`
	MaxMigrationTokens    uint64       `json:"max_migration_tokens"`
	MaxStreamDuration     string       `json:"max_stream_duration"`
	OrphanPolicy          OrphanPolicy `json:"orphan_policy"`
	OrphanTimeout         string       `json:"orphan_timeout"`
	AllowCrossVersion     bool         `json:"allow_cross_version"`
	AllowStructuredResume bool         `json:"allow_structured_resume"`
	SeamWindowBytes       int          `json:"seam_window_bytes"`
	JournalTTL            string       `json:"journal_ttl"`
}

type streamPolicy struct {
	MaxMigrations         int
	MaxMigrationTokens    uint64
	MaxStreamDuration     time.Duration
	OrphanPolicy          OrphanPolicy
	OrphanTimeout         time.Duration
	AllowCrossVersion     bool
	AllowStructuredResume bool
	SeamWindowBytes       int
	JournalTTL            time.Duration
	TemplateMode          conformance.TemplateMode
}

func streamPolicyFromConfig(config Config) streamPolicy {
	return streamPolicy{
		MaxMigrations:         config.MaxMigrations,
		MaxMigrationTokens:    config.MaxMigrationTokens,
		MaxStreamDuration:     config.MaxStreamDuration,
		OrphanPolicy:          config.OrphanPolicy,
		OrphanTimeout:         config.OrphanTimeout,
		AllowCrossVersion:     config.AllowCrossVersion,
		AllowStructuredResume: config.AllowStructuredResume,
		SeamWindowBytes:       config.SeamWindowBytes,
		JournalTTL:            config.JournalTTL,
		TemplateMode:          config.TemplateMode,
	}
}

type routeBackendInput struct {
	ID              string              `json:"id"`
	URL             string              `json:"url"`
	ModelVersion    string              `json:"model_version,omitempty"`
	TemplateVerdict conformance.Verdict `json:"template_verdict"`
	PodNamespace    string              `json:"pod_namespace,omitempty"`
	PodName         string              `json:"pod_name,omitempty"`
}

type routeApplyResult struct {
	Route              string   `json:"route"`
	Model              string   `json:"model"`
	AppliedGeneration  int64    `json:"applied_generation"`
	BackendCount       int      `json:"backend_count"`
	DrainingBackends   int      `json:"draining_backends"`
	DrainingBackendIDs []string `json:"draining_backend_ids,omitempty"`
	ActiveStreams      int      `json:"active_streams"`
}

type routeSnapshot struct {
	model      string
	uid        string
	generation int64
	deleted    bool
	digest     [sha256.Size]byte
	policy     streamPolicy
	backends   []backend.Backend
}

type routeBackendRegistry struct {
	mu       sync.Mutex
	pool     *backend.Pool
	static   []backend.Backend
	routes   map[string]routeSnapshot
	routeIDs map[string]map[backend.ID]struct{}
}

func newRouteBackendRegistry(pool *backend.Pool) (*routeBackendRegistry, error) {
	if pool == nil {
		return nil, errors.New("route backend registry requires a backend pool")
	}
	states := pool.List()
	static := make([]backend.Backend, len(states))
	for index := range states {
		static[index] = states[index].Backend
	}
	return &routeBackendRegistry{
		pool: pool, static: static, routes: make(map[string]routeSnapshot),
		routeIDs: make(map[string]map[backend.ID]struct{}),
	}, nil
}

func (registry *routeBackendRegistry) apply(
	route string,
	update routeBackendUpdate,
) (routeApplyResult, error) {
	normalized, err := normalizeRouteSnapshot(route, update)
	if err != nil {
		return routeApplyResult{}, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	existing, exists := registry.routes[route]
	if exists {
		if normalized.uid != existing.uid {
			if !existing.deleted {
				return registry.resultLocked(route, existing), errRouteUIDConflict
			}
		} else {
			if normalized.generation < existing.generation ||
				(existing.deleted && !normalized.deleted) {
				return registry.resultLocked(route, existing), errRouteGenerationStale
			}
			if normalized.generation == existing.generation && normalized.digest == existing.digest {
				return registry.resultLocked(route, existing), nil
			}
		}
	}

	candidateRoutes := make(map[string]routeSnapshot, len(registry.routes)+1)
	for key, snapshot := range registry.routes {
		candidateRoutes[key] = snapshot
	}
	candidateRoutes[route] = normalized
	combined, err := registry.combined(candidateRoutes)
	if err != nil {
		return routeApplyResult{}, err
	}
	if err := registry.pool.ReplaceReady(combined); err != nil {
		return routeApplyResult{}, fmt.Errorf("apply route backend set: %w", err)
	}
	ids := registry.routeIDs[route]
	if ids == nil {
		ids = make(map[backend.ID]struct{}, len(normalized.backends))
		registry.routeIDs[route] = ids
	}
	for _, item := range normalized.backends {
		ids[item.ID] = struct{}{}
	}
	registry.routes[route] = normalized
	return registry.resultLocked(route, normalized), nil
}

func (registry *routeBackendRegistry) combined(routes map[string]routeSnapshot) ([]backend.Backend, error) {
	liveRoutes := 0
	for _, snapshot := range routes {
		if !snapshot.deleted {
			liveRoutes++
		}
	}
	if liveRoutes == 0 {
		return slices.Clone(registry.static), nil
	}
	count := 0
	for _, snapshot := range routes {
		if !snapshot.deleted {
			count += len(snapshot.backends)
		}
	}
	combined := make([]backend.Backend, 0, count)
	seen := make(map[backend.ID]string, count)
	models := make(map[string]string, len(routes))
	keys := make([]string, 0, len(routes))
	for key := range routes {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, route := range keys {
		if routes[route].deleted {
			continue
		}
		if previous, ok := models[routes[route].model]; ok {
			return nil, fmt.Errorf("model %q is shared by routes %q and %q", routes[route].model, previous, route)
		}
		models[routes[route].model] = route
		for _, item := range routes[route].backends {
			if previous, ok := seen[item.ID]; ok {
				return nil, fmt.Errorf("backend ID %q is shared by routes %q and %q", item.ID, previous, route)
			}
			seen[item.ID] = route
			combined = append(combined, item)
		}
	}
	return combined, nil
}

func (registry *routeBackendRegistry) policyForModel(model string) (streamPolicy, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, snapshot := range registry.routes {
		if !snapshot.deleted && snapshot.model == model {
			return snapshot.policy, true
		}
	}
	return streamPolicy{}, false
}

// acquireModel linearizes request admission with route replacement and
// deletion. In particular, a deleted model cannot race into the standalone
// wildcard backend after its dynamic backend set has been retired.
func (registry *routeBackendRegistry) acquireModel(
	owner string,
	model string,
	excluded ...backend.ID,
) (*backend.Lease, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	live, tombstoned := false, false
	for _, snapshot := range registry.routes {
		if snapshot.model != model {
			continue
		}
		if snapshot.deleted {
			tombstoned = true
		} else {
			live = true
		}
	}
	if tombstoned && !live {
		return nil, backend.ErrNoEligibleBackend
	}
	return registry.pool.AcquireModel(owner, model, excluded...)
}

func (registry *routeBackendRegistry) resultLocked(route string, snapshot routeSnapshot) routeApplyResult {
	result := routeApplyResult{
		Route: route, Model: snapshot.model, AppliedGeneration: snapshot.generation,
		BackendCount: len(snapshot.backends),
	}
	ids := registry.routeIDs[route]
	retained := registry.pool.ListRetained()
	present := make(map[backend.ID]struct{}, len(retained))
	for _, state := range retained {
		present[state.ID] = struct{}{}
		if _, ok := ids[state.ID]; !ok {
			continue
		}
		if state.Draining {
			result.DrainingBackends++
			if result.DrainingBackends <= maxRouteResultDrainingBackendIDs {
				result.DrainingBackendIDs = append(result.DrainingBackendIDs, state.ID.String())
			}
		}
		result.ActiveStreams += state.InFlight
	}
	if result.DrainingBackends > maxRouteResultDrainingBackendIDs {
		// Omission is the bounded compatibility fallback. The count remains
		// authoritative, but callers cannot accidentally treat a prefix as a
		// complete identity set.
		result.DrainingBackendIDs = nil
	}
	for id := range ids {
		if _, ok := present[id]; !ok {
			delete(ids, id)
		}
	}
	return result
}

func normalizeRouteSnapshot(route string, update routeBackendUpdate) (routeSnapshot, error) {
	if err := validateRouteIdentity(route); err != nil {
		return routeSnapshot{}, err
	}
	if err := validateRouteText("model", update.Model, 512); err != nil {
		return routeSnapshot{}, err
	}
	if err := validateRouteText("uid", update.UID, 128); err != nil {
		return routeSnapshot{}, err
	}
	if update.ObservedGeneration <= 0 {
		return routeSnapshot{}, errors.New("observed_generation must be positive")
	}
	if len(update.Backends) > maxRouteBackends {
		return routeSnapshot{}, fmt.Errorf("backends exceeds the limit of %d", maxRouteBackends)
	}
	if update.Deleted && len(update.Backends) != 0 {
		return routeSnapshot{}, errors.New("deleted route update must contain no backends")
	}

	items := make([]backend.Backend, 0, len(update.Backends))
	canonical := routeBackendUpdate{
		Model: update.Model, UID: update.UID, ObservedGeneration: update.ObservedGeneration,
		Deleted:  update.Deleted,
		Policy:   update.Policy,
		Backends: make([]routeBackendInput, 0, len(update.Backends)),
	}
	policy, err := normalizeRoutePolicy(update.Policy)
	if err != nil {
		return routeSnapshot{}, err
	}
	seen := make(map[backend.ID]struct{}, len(update.Backends))
	for index, input := range update.Backends {
		if err := validateRouteText("backend id", input.ID, 512); err != nil {
			return routeSnapshot{}, fmt.Errorf("backends[%d]: %w", index, err)
		}
		parsed, err := url.Parse(input.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
			parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return routeSnapshot{}, fmt.Errorf("backends[%d] has an invalid HTTP(S) origin URL", index)
		}
		if !input.TemplateVerdict.Valid() {
			return routeSnapshot{}, fmt.Errorf("backends[%d] has invalid template_verdict %q", index, input.TemplateVerdict)
		}
		if (input.PodNamespace == "") != (input.PodName == "") {
			return routeSnapshot{}, fmt.Errorf("backends[%d] pod_namespace and pod_name must be set together", index)
		}
		if input.PodNamespace != "" {
			if !routeNamespacePattern.MatchString(input.PodNamespace) || !routeNamePattern.MatchString(input.PodName) {
				return routeSnapshot{}, fmt.Errorf("backends[%d] has invalid pod identity", index)
			}
		}
		id := backend.ID(input.ID)
		if _, duplicate := seen[id]; duplicate {
			return routeSnapshot{}, fmt.Errorf("backends[%d] duplicates backend ID %q", index, id)
		}
		seen[id] = struct{}{}
		items = append(items, backend.Backend{
			ID: id, URL: parsed, Model: update.Model, ModelVersion: input.ModelVersion,
			TemplateVerdict: input.TemplateVerdict,
			PodNamespace:    input.PodNamespace, PodName: input.PodName,
		})
		input.URL = parsed.String()
		canonical.Backends = append(canonical.Backends, input)
	}
	slices.SortFunc(items, func(left, right backend.Backend) int {
		return strings.Compare(left.ID.String(), right.ID.String())
	})
	slices.SortFunc(canonical.Backends, func(left, right routeBackendInput) int {
		return strings.Compare(left.ID, right.ID)
	})
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return routeSnapshot{}, fmt.Errorf("canonicalize route backend set: %w", err)
	}
	return routeSnapshot{
		model: update.Model, uid: update.UID, generation: update.ObservedGeneration,
		deleted: update.Deleted, digest: sha256.Sum256(encoded), policy: policy, backends: items,
	}, nil
}

func normalizeRoutePolicy(input routePolicyInput) (streamPolicy, error) {
	if input.MaxMigrations < 0 {
		return streamPolicy{}, errors.New("policy.max_migrations cannot be negative")
	}
	if input.SeamWindowBytes <= 0 {
		return streamPolicy{}, errors.New("policy.seam_window_bytes must be positive")
	}
	if !input.OrphanPolicy.valid() {
		return streamPolicy{}, errors.New("policy.orphan_policy is invalid")
	}
	maxDuration, err := time.ParseDuration(input.MaxStreamDuration)
	if err != nil || maxDuration <= 0 {
		return streamPolicy{}, errors.New("policy.max_stream_duration must be a positive duration")
	}
	orphanTimeout, err := time.ParseDuration(input.OrphanTimeout)
	if err != nil || orphanTimeout <= 0 {
		return streamPolicy{}, errors.New("policy.orphan_timeout must be a positive duration")
	}
	journalTTL, err := time.ParseDuration(input.JournalTTL)
	if err != nil || journalTTL <= 0 || journalTTL > time.Duration(math.MaxInt64/2) {
		return streamPolicy{}, errors.New("policy.journal_ttl must be a positive safely bounded duration")
	}
	return streamPolicy{
		MaxMigrations: input.MaxMigrations, MaxMigrationTokens: input.MaxMigrationTokens,
		MaxStreamDuration: maxDuration, OrphanPolicy: input.OrphanPolicy,
		OrphanTimeout: orphanTimeout, AllowCrossVersion: input.AllowCrossVersion,
		AllowStructuredResume: input.AllowStructuredResume,
		SeamWindowBytes:       input.SeamWindowBytes, JournalTTL: journalTTL,
		TemplateMode: conformance.TemplateStrict,
	}, nil
}

func validateRouteIdentity(route string) error {
	parts := strings.Split(route, "/")
	if len(parts) != 2 || !routeNamespacePattern.MatchString(parts[0]) ||
		!routeNamePattern.MatchString(parts[1]) {
		return errors.New("route must be a canonical namespace/name")
	}
	return nil
}

func validateRouteText(field, value string, limit int) error {
	if value == "" || len(value) > limit || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must be 1-%d unpadded bytes", field, limit)
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("%s contains a control character", field)
		}
	}
	return nil
}
