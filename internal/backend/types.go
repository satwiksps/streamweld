package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/streamweld/streamweld/internal/conformance"
)

const (
	// DefaultQuarantineWindow is the protocol-defined passive-failure ejection
	// period.
	DefaultQuarantineWindow = 5 * time.Second
	// DefaultProbeInterval is the default cadence for active health checks.
	DefaultProbeInterval = 10 * time.Second
	// DefaultProbeTimeout bounds one backend health request.
	DefaultProbeTimeout = 2 * time.Second
	// DefaultProbeConcurrency bounds simultaneous health requests.
	DefaultProbeConcurrency = 16
)

var (
	// ErrInvalidConfig means one or more pool settings are invalid.
	ErrInvalidConfig = errors.New("backend: invalid pool configuration")
	// ErrInvalidBackend means a backend ID, URL, or verdict is invalid.
	ErrInvalidBackend = errors.New("backend: invalid backend")
	// ErrNotFound means the requested backend is not in the current pool.
	ErrNotFound = errors.New("backend: backend not found")
	// ErrNoEligibleBackend means no healthy, non-draining, non-quarantined,
	// non-excluded backend can accept a lease.
	ErrNoEligibleBackend = errors.New("backend: no eligible backend")
	// ErrInvalidSelection means an injected chooser returned an invalid index.
	ErrInvalidSelection = errors.New("backend: chooser returned an invalid index")
	// ErrNotDraining means WaitDrained was called for an active backend.
	ErrNotDraining = errors.New("backend: backend is not draining")
	// ErrHealthChecksRunning means RunHealthChecks already owns the pool's
	// active-probe loop.
	ErrHealthChecksRunning = errors.New("backend: health checks already running")
	// ErrLeaseIDExhausted means the process-local lease counter is exhausted.
	ErrLeaseIDExhausted = errors.New("backend: lease ID exhausted")
	// ErrInvalidContext means a nil context was supplied.
	ErrInvalidContext = errors.New("backend: nil context")
	// ErrDrainClosed means a retained drain was used after it released its
	// record pin.
	ErrDrainClosed = errors.New("backend: retained drain is closed")
)

// ID is an operator-assigned opaque backend identity. Deployments normally use
// the canonical host:port registered in the backend pool.
type ID string

// String returns the backend's opaque identifier.
func (id ID) String() string { return string(id) }

// Validate rejects empty, padded, or control-character backend identities.
func (id ID) Validate() error {
	value := string(id)
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: ID must be non-empty and unpadded", ErrInvalidBackend)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: ID contains a control character", ErrInvalidBackend)
		}
	}
	return nil
}

// Health is the latest active-probe health state.
type Health string

const (
	// HealthUnknown means the backend has not completed a current successful
	// probe.
	HealthUnknown Health = "unknown"
	// HealthHealthy means the latest active probe succeeded.
	HealthHealthy Health = "healthy"
	// HealthUnhealthy means the latest active probe failed.
	HealthUnhealthy Health = "unhealthy"
)

// Valid reports whether health is a defined state.
func (health Health) Valid() bool {
	return health == HealthUnknown || health == HealthHealthy || health == HealthUnhealthy
}

// Backend is the controller-supplied, updateable identity and compatibility
// metadata for one inference server. An empty TemplateVerdict is normalized to
// conformance.VerdictUnknown.
type Backend struct {
	ID              ID                  `json:"id"`
	URL             *url.URL            `json:"-"`
	HealthURL       *url.URL            `json:"-"`
	Model           string              `json:"model,omitempty"`
	ModelVersion    string              `json:"model_version"`
	TemplateVerdict conformance.Verdict `json:"template_verdict"`
	PodNamespace    string              `json:"pod_namespace,omitempty"`
	PodName         string              `json:"pod_name,omitempty"`
}

// State is an immutable point-in-time backend snapshot.
type State struct {
	Backend
	Health           Health    `json:"health"`
	Draining         bool      `json:"draining"`
	Quarantined      bool      `json:"quarantined"`
	QuarantinedUntil time.Time `json:"quarantined_until,omitempty"`
	InFlight         int       `json:"in_flight"`
	LastProbeAt      time.Time `json:"last_probe_at,omitempty"`
	LastFailureAt    time.Time `json:"last_failure_at,omitempty"`
	LastProbeError   string    `json:"last_probe_error,omitempty"`
}

// Binding identifies one in-flight lease captured when a backend begins
// draining. Owner is an opaque stream/attempt identity supplied by the caller.
type Binding struct {
	LeaseID uint64 `json:"lease_id"`
	Owner   string `json:"owner,omitempty"`
}

// DrainSnapshot is the atomic result of excluding a backend from new
// selection and snapshotting its current in-flight bindings.
type DrainSnapshot struct {
	Backend  State     `json:"backend"`
	Bindings []Binding `json:"bindings"`
}

// Transition describes a health or quarantine state update. Bindings is
// populated when the update newly makes a backend unavailable, allowing the
// caller to trigger migration without another racy lookup.
type Transition struct {
	Backend  State     `json:"backend"`
	Changed  bool      `json:"changed"`
	Bindings []Binding `json:"bindings,omitempty"`
}

// ProbeResult reports one active health check and whether its result was
// applied. Results for concurrently removed or updated endpoints are safely
// discarded with Applied false.
type ProbeResult struct {
	ID         ID         `json:"id"`
	At         time.Time  `json:"at"`
	Healthy    bool       `json:"healthy"`
	Applied    bool       `json:"applied"`
	Transition Transition `json:"transition"`
	Err        error      `json:"-"`
}

// ChooseFunc chooses an index from ID-sorted eligible snapshots. The callback
// runs without pool locks and must return an index in [0, len(candidates)).
type ChooseFunc func(candidates []State) int

// ProbeFunc performs one active health check. The callback runs without pool
// locks. Returning nil marks the backend healthy; any error marks it unhealthy
// unless the parent probe context itself was canceled.
type ProbeFunc func(ctx context.Context, backend Backend) error

// Ticker is the minimal ticker contract used by the active health loop.
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

// TickerFactory creates an interval ticker. It is injectable for deterministic
// health-loop tests.
type TickerFactory func(interval time.Duration) Ticker

// Config controls selection, passive quarantine, and active health probes.
type Config struct {
	QuarantineWindow time.Duration
	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	ProbeConcurrency int
	Clock            func() time.Time
	Choose           ChooseFunc
	Probe            ProbeFunc
	TickerFactory    TickerFactory
	HTTPClient       *http.Client
}

// DefaultConfig returns production defaults and a hardened HTTP health client.
func DefaultConfig() Config {
	return Config{
		QuarantineWindow: DefaultQuarantineWindow,
		ProbeInterval:    DefaultProbeInterval,
		ProbeTimeout:     DefaultProbeTimeout,
		ProbeConcurrency: DefaultProbeConcurrency,
		Clock:            time.Now,
		TickerFactory:    newRealTicker,
		HTTPClient: &http.Client{
			Transport: http.DefaultTransport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}
