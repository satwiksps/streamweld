package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streamweld/streamweld/internal/conformance"
)

// Pool is a dynamically updateable backend registry. All returned backend and
// state values are deep snapshots, and Pool is safe for concurrent use.
type Pool struct {
	mu      sync.Mutex
	config  Config
	records map[ID]*record

	nextSelection atomic.Uint64
	running       atomic.Bool
	nextLeaseID   uint64
}

type record struct {
	backend          Backend
	revision         uint64
	health           Health
	draining         bool
	retired          bool
	quarantinedUntil time.Time
	lastProbeAt      time.Time
	lastFailureAt    time.Time
	lastProbeError   string
	leases           map[uint64]string
	waiters          int
	changed          chan struct{}
}

// Lease reserves one backend for an in-flight producer attempt. Release is
// concurrency-safe and idempotent.
type Lease struct {
	pool     *Pool
	record   *record
	id       uint64
	owner    string
	backend  State
	released atomic.Bool
}

// NewPool validates config and atomically installs the initial backend set.
func NewPool(config Config, initial ...Backend) (*Pool, error) {
	config = configWithDefaults(config)
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	pool := &Pool{config: config, records: make(map[ID]*record)}
	if err := pool.Replace(initial); err != nil {
		return nil, err
	}
	return pool, nil
}

// Upsert adds a backend or updates its controller-owned metadata without
// restarting the pool. Runtime drain and quarantine state is preserved. A URL
// change resets active health to unknown until the new endpoint is probed.
func (pool *Pool) Upsert(backend Backend) (State, error) {
	normalized, err := normalizeBackend(backend)
	if err != nil {
		return State{}, err
	}
	now := pool.now()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[normalized.ID]
	if record == nil {
		record = newRecord(normalized)
		pool.records[normalized.ID] = record
	} else {
		wasRetired := record.retired
		updateRecord(record, normalized)
		if wasRetired {
			record.revision++
		}
		record.retired = false
		pool.notifyLocked(record)
	}
	return pool.stateLocked(record, now), nil
}

// Replace atomically reconciles the complete controller-supplied backend set.
// Missing backends stop receiving leases immediately. A missing backend with
// active leases remains internally retained until every lease is released.
func (pool *Pool) Replace(backends []Backend) error {
	normalized := make([]Backend, len(backends))
	seen := make(map[ID]struct{}, len(backends))
	for index, backend := range backends {
		value, err := normalizeBackend(backend)
		if err != nil {
			return fmt.Errorf("replace backend %d: %w", index, err)
		}
		if _, exists := seen[value.ID]; exists {
			return fmt.Errorf("%w: duplicate ID %q", ErrInvalidBackend, value.ID)
		}
		seen[value.ID] = struct{}{}
		normalized[index] = value
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, backend := range normalized {
		record := pool.records[backend.ID]
		if record == nil {
			pool.records[backend.ID] = newRecord(backend)
			continue
		}
		wasRetired := record.retired
		updateRecord(record, backend)
		if wasRetired {
			record.revision++
		}
		record.retired = false
		pool.notifyLocked(record)
	}
	for id, record := range pool.records {
		if _, exists := seen[id]; exists || record.retired {
			continue
		}
		record.retired = true
		record.draining = true
		pool.notifyLocked(record)
		pool.cleanupRetiredLocked(record)
	}
	return nil
}

// Remove excludes a backend immediately and retires it after its last lease is
// released. It reports false when the backend was already absent.
func (pool *Pool) Remove(id ID) (bool, error) {
	if err := id.Validate(); err != nil {
		return false, err
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return false, nil
	}
	record.retired = true
	record.draining = true
	pool.notifyLocked(record)
	pool.cleanupRetiredLocked(record)
	return true, nil
}

// Get returns one immutable backend snapshot.
func (pool *Pool) Get(id ID) (State, error) {
	if err := id.Validate(); err != nil {
		return State{}, err
	}
	now := pool.now()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return State{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return pool.stateLocked(record, now), nil
}

// List returns all current controller-supplied backends sorted by ID.
func (pool *Pool) List() []State {
	now := pool.now()
	pool.mu.Lock()
	states := make([]State, 0, len(pool.records))
	for _, record := range pool.records {
		if !record.retired {
			states = append(states, pool.stateLocked(record, now))
		}
	}
	pool.mu.Unlock()
	sort.Slice(states, func(left, right int) bool { return states[left].ID < states[right].ID })
	return states
}

// Acquire selects and atomically leases a healthy, non-draining,
// non-quarantined backend not present in excluded. Owner is an opaque stream or
// attempt identity returned in drain snapshots.
func (pool *Pool) Acquire(owner string, excluded ...ID) (*Lease, error) {
	exclusions := make(map[ID]struct{}, len(excluded))
	for _, id := range excluded {
		if err := id.Validate(); err != nil {
			return nil, fmt.Errorf("exclude backend: %w", err)
		}
		exclusions[id] = struct{}{}
	}

	for attempt := 0; attempt < 64; attempt++ {
		now := pool.now()
		pool.mu.Lock()
		candidates := pool.candidatesLocked(exclusions, now)
		pool.mu.Unlock()
		if len(candidates) == 0 {
			return nil, ErrNoEligibleBackend
		}

		var choice int
		if pool.config.Choose != nil {
			choice = pool.config.Choose(cloneStates(candidates))
		} else {
			choice = int((pool.nextSelection.Add(1) - 1) % uint64(len(candidates)))
		}
		if choice < 0 || choice >= len(candidates) {
			return nil, fmt.Errorf("%w: got %d for %d candidates", ErrInvalidSelection, choice, len(candidates))
		}

		chosen := candidates[choice].ID
		now = pool.now()
		pool.mu.Lock()
		record := pool.records[chosen]
		if record == nil || !pool.eligibleLocked(record, exclusions, now) {
			pool.mu.Unlock()
			continue
		}
		lease, err := pool.acquireLocked(record, owner, now)
		pool.mu.Unlock()
		return lease, err
	}
	return nil, ErrNoEligibleBackend
}

// AcquireID atomically leases a specific backend if it is currently eligible.
func (pool *Pool) AcquireID(id ID, owner string) (*Lease, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	now := pool.now()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !pool.eligibleLocked(record, nil, now) {
		return nil, fmt.Errorf("%w: %s", ErrNoEligibleBackend, id)
	}
	return pool.acquireLocked(record, owner, now)
}

// ID returns the process-local identifier of this lease.
func (lease *Lease) ID() uint64 {
	if lease == nil {
		return 0
	}
	return lease.id
}

// Owner returns the opaque binding identity supplied to Acquire.
func (lease *Lease) Owner() string {
	if lease == nil {
		return ""
	}
	return lease.owner
}

// Backend returns the immutable backend snapshot reserved by this lease.
func (lease *Lease) Backend() State {
	if lease == nil {
		return State{}
	}
	return cloneState(lease.backend)
}

// Release returns the in-flight lease. Repeated and concurrent calls are safe.
func (lease *Lease) Release() {
	if lease == nil || !lease.released.CompareAndSwap(false, true) {
		return
	}
	pool := lease.pool
	pool.mu.Lock()
	if _, exists := lease.record.leases[lease.id]; exists {
		delete(lease.record.leases, lease.id)
		pool.notifyLocked(lease.record)
		pool.cleanupRetiredLocked(lease.record)
	}
	pool.mu.Unlock()
}

// SetHealth applies an externally determined health state. A healthy to
// unhealthy transition atomically snapshots affected bindings.
func (pool *Pool) SetHealth(id ID, health Health) (Transition, error) {
	if err := id.Validate(); err != nil {
		return Transition{}, err
	}
	if !health.Valid() {
		return Transition{}, fmt.Errorf("%w: invalid health %q", ErrInvalidBackend, health)
	}
	now := pool.now()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return Transition{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	oldHealth := record.health
	changed := oldHealth != health
	if changed {
		record.health = health
		record.lastProbeAt = now
		if health != HealthUnhealthy {
			record.lastProbeError = ""
		}
		pool.notifyLocked(record)
	}
	transition := Transition{Backend: pool.stateLocked(record, now), Changed: changed}
	if health != HealthHealthy && len(record.leases) != 0 {
		transition.Bindings = bindingsLocked(record)
	}
	return transition, nil
}

// MarkPassiveFailure quarantines a backend for at least the configured window
// without overwriting its independent active-probe health state.
func (pool *Pool) MarkPassiveFailure(id ID) (Transition, error) {
	if err := id.Validate(); err != nil {
		return Transition{}, err
	}
	now := pool.now()
	until := now.Add(pool.config.QuarantineWindow)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return Transition{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	wasQuarantined := now.Before(record.quarantinedUntil)
	if record.quarantinedUntil.Before(until) {
		record.quarantinedUntil = until
	}
	record.lastFailureAt = now
	pool.notifyLocked(record)
	transition := Transition{Backend: pool.stateLocked(record, now), Changed: true}
	if !wasQuarantined {
		transition.Bindings = bindingsLocked(record)
	}
	return transition, nil
}

// MarkDraining atomically excludes a backend from new selection and snapshots
// its in-flight bindings. Calling it repeatedly is safe.
func (pool *Pool) MarkDraining(id ID) (DrainSnapshot, error) {
	if err := id.Validate(); err != nil {
		return DrainSnapshot{}, err
	}
	now := pool.now()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return DrainSnapshot{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !record.draining {
		record.draining = true
		pool.notifyLocked(record)
	}
	return DrainSnapshot{
		Backend:  pool.stateLocked(record, now),
		Bindings: bindingsLocked(record),
	}, nil
}

// Undrain administratively returns a backend to service. Health and quarantine
// gates still apply.
func (pool *Pool) Undrain(id ID) (State, error) {
	if err := id.Validate(); err != nil {
		return State{}, err
	}
	now := pool.now()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	record := pool.records[id]
	if record == nil || record.retired {
		return State{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if record.draining {
		record.draining = false
		pool.notifyLocked(record)
	}
	return pool.stateLocked(record, now), nil
}

// WaitDrained waits until a previously marked backend has no in-flight leases.
// On context expiry it returns the latest state, including the remaining count.
func (pool *Pool) WaitDrained(ctx context.Context, id ID) (State, error) {
	if ctx == nil {
		return State{}, ErrInvalidContext
	}
	if err := id.Validate(); err != nil {
		return State{}, err
	}

	pool.mu.Lock()
	record := pool.records[id]
	if record == nil || record.retired {
		pool.mu.Unlock()
		return State{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !record.draining {
		pool.mu.Unlock()
		return State{}, fmt.Errorf("%w: %s", ErrNotDraining, id)
	}
	record.waiters++
	pool.mu.Unlock()

	defer func() {
		pool.mu.Lock()
		record.waiters--
		pool.cleanupRetiredLocked(record)
		pool.mu.Unlock()
	}()

	for {
		now := pool.now()
		pool.mu.Lock()
		state := pool.stateLocked(record, now)
		if !record.draining {
			pool.mu.Unlock()
			return state, fmt.Errorf("%w: %s", ErrNotDraining, id)
		}
		if len(record.leases) == 0 {
			pool.mu.Unlock()
			return state, nil
		}
		changed := record.changed
		pool.mu.Unlock()

		select {
		case <-ctx.Done():
			now = pool.now()
			pool.mu.Lock()
			state = pool.stateLocked(record, now)
			pool.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return state, err
			}
			return state, errors.New("backend: drain context ended")
		case <-changed:
		}
	}
}

func (pool *Pool) acquireLocked(record *record, owner string, now time.Time) (*Lease, error) {
	if pool.nextLeaseID == math.MaxUint64 {
		return nil, ErrLeaseIDExhausted
	}
	pool.nextLeaseID++
	id := pool.nextLeaseID
	record.leases[id] = owner
	pool.notifyLocked(record)
	state := pool.stateLocked(record, now)
	return &Lease{pool: pool, record: record, id: id, owner: owner, backend: state}, nil
}

func (pool *Pool) candidatesLocked(excluded map[ID]struct{}, now time.Time) []State {
	candidates := make([]State, 0, len(pool.records))
	for _, record := range pool.records {
		if pool.eligibleLocked(record, excluded, now) {
			candidates = append(candidates, pool.stateLocked(record, now))
		}
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].ID < candidates[right].ID })
	return candidates
}

func (pool *Pool) eligibleLocked(record *record, excluded map[ID]struct{}, now time.Time) bool {
	if record.retired || record.health != HealthHealthy || record.draining || now.Before(record.quarantinedUntil) {
		return false
	}
	_, blocked := excluded[record.backend.ID]
	return !blocked
}

func (pool *Pool) stateLocked(record *record, now time.Time) State {
	return State{
		Backend:          cloneBackend(record.backend),
		Health:           record.health,
		Draining:         record.draining,
		Quarantined:      now.Before(record.quarantinedUntil),
		QuarantinedUntil: record.quarantinedUntil,
		InFlight:         len(record.leases),
		LastProbeAt:      record.lastProbeAt,
		LastFailureAt:    record.lastFailureAt,
		LastProbeError:   record.lastProbeError,
	}
}

func (pool *Pool) notifyLocked(record *record) {
	close(record.changed)
	record.changed = make(chan struct{})
}

func (pool *Pool) cleanupRetiredLocked(record *record) {
	if record.retired && len(record.leases) == 0 && record.waiters == 0 && pool.records[record.backend.ID] == record {
		delete(pool.records, record.backend.ID)
	}
}

func (pool *Pool) now() time.Time { return pool.config.Clock().UTC() }

func newRecord(backend Backend) *record {
	return &record{
		backend:  backend,
		revision: 1,
		health:   HealthUnknown,
		leases:   make(map[uint64]string),
		changed:  make(chan struct{}),
	}
}

func updateRecord(record *record, backend Backend) {
	urlChanged := record.backend.URL.String() != backend.URL.String()
	record.backend = backend
	if urlChanged {
		record.revision++
		record.health = HealthUnknown
		record.lastProbeAt = time.Time{}
		record.lastProbeError = ""
	}
}

func bindingsLocked(record *record) []Binding {
	bindings := make([]Binding, 0, len(record.leases))
	for id, owner := range record.leases {
		bindings = append(bindings, Binding{LeaseID: id, Owner: owner})
	}
	sort.Slice(bindings, func(left, right int) bool { return bindings[left].LeaseID < bindings[right].LeaseID })
	return bindings
}

func normalizeBackend(backend Backend) (Backend, error) {
	if err := backend.ID.Validate(); err != nil {
		return Backend{}, err
	}
	if backend.URL == nil {
		return Backend{}, fmt.Errorf("%w: backend %q URL is nil", ErrInvalidBackend, backend.ID)
	}
	clonedURL := cloneURL(backend.URL)
	clonedURL.Scheme = strings.ToLower(clonedURL.Scheme)
	if (clonedURL.Scheme != "http" && clonedURL.Scheme != "https") || clonedURL.Host == "" {
		return Backend{}, fmt.Errorf("%w: backend %q URL must be an absolute HTTP(S) URL", ErrInvalidBackend, backend.ID)
	}
	if clonedURL.User != nil || clonedURL.Fragment != "" {
		return Backend{}, fmt.Errorf("%w: backend %q URL cannot contain credentials or a fragment", ErrInvalidBackend, backend.ID)
	}
	if backend.TemplateVerdict == "" {
		backend.TemplateVerdict = conformance.VerdictUnknown
	}
	if !backend.TemplateVerdict.Valid() {
		return Backend{}, fmt.Errorf("%w: backend %q has invalid template verdict %q", ErrInvalidBackend, backend.ID, backend.TemplateVerdict)
	}
	backend.URL = clonedURL
	return backend, nil
}

func cloneBackend(backend Backend) Backend {
	backend.URL = cloneURL(backend.URL)
	return backend
}

func cloneURL(source *url.URL) *url.URL {
	if source == nil {
		return nil
	}
	cloned := *source
	if source.User != nil {
		user := *source.User
		cloned.User = &user
	}
	return &cloned
}

func cloneState(state State) State {
	state.Backend = cloneBackend(state.Backend)
	return state
}

func cloneStates(states []State) []State {
	cloned := make([]State, len(states))
	for index, state := range states {
		cloned[index] = cloneState(state)
	}
	return cloned
}

func configWithDefaults(config Config) Config {
	defaults := DefaultConfig()
	if config.QuarantineWindow == 0 {
		config.QuarantineWindow = defaults.QuarantineWindow
	}
	if config.ProbeInterval == 0 {
		config.ProbeInterval = defaults.ProbeInterval
	}
	if config.ProbeTimeout == 0 {
		config.ProbeTimeout = defaults.ProbeTimeout
	}
	if config.ProbeConcurrency == 0 {
		config.ProbeConcurrency = defaults.ProbeConcurrency
	}
	if config.Clock == nil {
		config.Clock = defaults.Clock
	}
	if config.TickerFactory == nil {
		config.TickerFactory = defaults.TickerFactory
	}
	if config.HTTPClient == nil {
		config.HTTPClient = defaults.HTTPClient
	}
	return config
}

func validateConfig(config Config) error {
	var problems []error
	if config.QuarantineWindow <= 0 {
		problems = append(problems, errors.New("quarantine window must be positive"))
	}
	if config.ProbeInterval <= 0 {
		problems = append(problems, errors.New("probe interval must be positive"))
	}
	if config.ProbeTimeout <= 0 {
		problems = append(problems, errors.New("probe timeout must be positive"))
	}
	if config.ProbeConcurrency <= 0 {
		problems = append(problems, errors.New("probe concurrency must be positive"))
	}
	if config.Clock == nil {
		problems = append(problems, errors.New("clock cannot be nil"))
	}
	if config.TickerFactory == nil {
		problems = append(problems, errors.New("ticker factory cannot be nil"))
	}
	if config.Probe == nil && config.HTTPClient == nil {
		problems = append(problems, errors.New("HTTP client cannot be nil without a custom prober"))
	}
	if len(problems) != 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(problems...))
	}
	return nil
}
