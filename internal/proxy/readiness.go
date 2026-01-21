package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

var errProxyShuttingDown = errors.New("proxy is shutting down")

// ReadinessChecker determines whether the configured serving backend is ready.
// A later backend pool can implement this interface without changing routing.
type ReadinessChecker interface {
	Check(context.Context) error
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

type backendReadinessChecker struct {
	client   *http.Client
	endpoint *url.URL
	timeout  time.Duration
}

func newBackendReadinessChecker(target *url.URL, transport http.RoundTripper, timeout time.Duration) *backendReadinessChecker {
	endpoint := *target
	endpoint.Path = "/health"
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.ForceQuery = false
	return &backendReadinessChecker{
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpoint: &endpoint,
		timeout:  timeout,
	}
}

func (c *backendReadinessChecker) Check(ctx context.Context) error {
	probeContext, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(probeContext, http.MethodGet, c.endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create backend health request: %w", err)
	}
	request.Header.Set("Accept", "application/json")

	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("probe backend health: %w", err)
	}
	_, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	closeErr := response.Body.Close()
	if drainErr != nil {
		return fmt.Errorf("read backend health response: %w", drainErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close backend health response: %w", closeErr)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("backend health returned HTTP %d", response.StatusCode)
	}
	return nil
}
