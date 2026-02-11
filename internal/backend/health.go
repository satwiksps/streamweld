package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxProbeErrorBytes = 1024

type probeTarget struct {
	backend  Backend
	revision uint64
}

// ProbeAll actively checks every current backend with bounded concurrency.
// Backend failures are returned as ProbeResult.Err and applied to pool health;
// only cancellation of the parent operation is returned as the method error.
func (pool *Pool) ProbeAll(ctx context.Context) ([]ProbeResult, error) {
	if ctx == nil {
		return nil, ErrInvalidContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	targets := pool.probeTargets()
	if len(targets) == 0 {
		return []ProbeResult{}, nil
	}

	jobs := make(chan probeTarget, len(targets))
	for _, target := range targets {
		jobs <- target
	}
	close(jobs)
	resultsChannel := make(chan ProbeResult, len(targets))
	workers := min(pool.config.ProbeConcurrency, len(targets))
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for target := range jobs {
				resultsChannel <- pool.probeOne(ctx, target)
			}
		}()
	}
	wait.Wait()
	close(resultsChannel)

	results := make([]ProbeResult, 0, len(targets))
	for result := range resultsChannel {
		results = append(results, result)
	}
	sort.Slice(results, func(left, right int) bool { return results[left].ID < results[right].ID })
	if err := ctx.Err(); err != nil {
		return results, err
	}
	return results, nil
}

// RunHealthChecks probes immediately and then at every configured interval
// until ctx ends. Only one active loop may run for a Pool.
func (pool *Pool) RunHealthChecks(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !pool.running.CompareAndSwap(false, true) {
		return ErrHealthChecksRunning
	}
	defer pool.running.Store(false)

	if _, err := pool.ProbeAll(ctx); err != nil {
		return err
	}
	ticker := pool.config.TickerFactory(pool.config.ProbeInterval)
	if ticker == nil {
		return fmt.Errorf("%w: ticker factory returned nil", ErrInvalidConfig)
	}
	defer ticker.Stop()
	ticks := ticker.Chan()
	if ticks == nil {
		return fmt.Errorf("%w: ticker returned a nil channel", ErrInvalidConfig)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ticks:
			if !ok {
				return errors.New("backend: health ticker stopped unexpectedly")
			}
			if _, err := pool.ProbeAll(ctx); err != nil {
				return err
			}
		}
	}
}

func (pool *Pool) probeTargets() []probeTarget {
	pool.mu.Lock()
	targets := make([]probeTarget, 0, len(pool.records))
	for _, record := range pool.records {
		if record.retired {
			continue
		}
		targets = append(targets, probeTarget{
			backend:  cloneBackend(record.backend),
			revision: record.revision,
		})
	}
	pool.mu.Unlock()
	sort.Slice(targets, func(left, right int) bool { return targets[left].backend.ID < targets[right].backend.ID })
	return targets
}

func (pool *Pool) probeOne(parent context.Context, target probeTarget) ProbeResult {
	probeContext, cancel := context.WithTimeout(parent, pool.config.ProbeTimeout)
	var err error
	if pool.config.Probe != nil {
		err = pool.config.Probe(probeContext, cloneBackend(target.backend))
	} else {
		err = pool.probeHTTP(probeContext, target.backend)
	}
	cancel()
	now := pool.now()
	result := ProbeResult{ID: target.backend.ID, At: now, Healthy: err == nil, Err: err}
	if parent.Err() != nil {
		return result
	}

	pool.mu.Lock()
	record := pool.records[target.backend.ID]
	if record == nil || record.retired || record.revision != target.revision {
		pool.mu.Unlock()
		return result
	}
	oldHealth := record.health
	newHealth := HealthHealthy
	if err != nil {
		newHealth = HealthUnhealthy
	}
	changed := oldHealth != newHealth
	record.health = newHealth
	record.lastProbeAt = now
	if err == nil {
		record.lastProbeError = ""
	} else {
		record.lastProbeError = boundedError(err)
	}
	pool.notifyLocked(record)
	transition := Transition{Backend: pool.stateLocked(record, now), Changed: changed}
	if oldHealth == HealthHealthy && newHealth != HealthHealthy {
		transition.Bindings = bindingsLocked(record)
	}
	pool.mu.Unlock()

	result.Applied = true
	result.Transition = transition
	return result
}

func (pool *Pool) probeHTTP(ctx context.Context, backend Backend) error {
	endpoint := cloneURL(backend.URL)
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/health"
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.ForceQuery = false
	endpoint.Fragment = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("construct health request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := pool.config.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("health request: %w", err)
	}
	if response.Body == nil {
		return errors.New("health response has nil body")
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 8<<10))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("health response status %d", response.StatusCode)
	}
	return nil
}

func boundedError(err error) string {
	message := strings.ToValidUTF8(err.Error(), "\uFFFD")
	if len(message) <= maxProbeErrorBytes {
		return message
	}
	message = message[:maxProbeErrorBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

type realTicker struct{ ticker *time.Ticker }

func newRealTicker(interval time.Duration) Ticker {
	return &realTicker{ticker: time.NewTicker(interval)}
}

func (ticker *realTicker) Chan() <-chan time.Time { return ticker.ticker.C }

func (ticker *realTicker) Stop() { ticker.ticker.Stop() }
