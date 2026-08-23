package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/satwiksps/streamweld/internal/backend"
)

var errProxyShuttingDown = errors.New("proxy is shutting down")

// ReadinessChecker determines whether the configured serving backend is ready.
// A later backend pool can implement this interface without changing routing.
type ReadinessChecker interface {
	Check(context.Context) error
}

type backendPoolReadinessChecker struct {
	pool       *backend.Pool
	transition func(backend.ProbeResult)
}

func newBackendPoolReadinessChecker(
	pool *backend.Pool,
	transition func(backend.ProbeResult),
) *backendPoolReadinessChecker {
	return &backendPoolReadinessChecker{pool: pool, transition: transition}
}

func (checker *backendPoolReadinessChecker) Check(ctx context.Context) error {
	if checker == nil || checker.pool == nil {
		return errors.New("backend pool is unavailable")
	}
	results, err := checker.pool.ProbeAll(ctx)
	if err != nil {
		return fmt.Errorf("probe backend pool: %w", err)
	}
	if checker.transition != nil {
		for _, result := range results {
			if len(result.Transition.Bindings) != 0 {
				checker.transition(result)
			}
		}
	}
	for _, state := range checker.pool.List() {
		if state.Health == backend.HealthHealthy && !state.Draining && !state.Quarantined {
			return nil
		}
	}
	return errors.New("backend pool has no healthy serving backend")
}

type readinessGate struct {
	checker      ReadinessChecker
	shuttingDown atomic.Bool
}

func newReadinessGate(checker ReadinessChecker) *readinessGate {
	return &readinessGate{checker: checker}
}

func (g *readinessGate) Check(ctx context.Context) error {
	if g.shuttingDown.Load() {
		return errProxyShuttingDown
	}
	if err := g.checker.Check(ctx); err != nil {
		return err
	}
	// Shutdown may have begun while the backend probe was in flight.
	if g.shuttingDown.Load() {
		return errProxyShuttingDown
	}
	return nil
}

func (g *readinessGate) BeginShutdown() {
	g.shuttingDown.Store(true)
}
