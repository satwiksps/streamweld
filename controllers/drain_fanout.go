package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultDrainFanoutConcurrency = 8
	drainResponseGracePeriod      = 2 * time.Second
)

// PodDrainFanoutConfig configures a bounded operator-to-proxy drain broadcast.
type PodDrainFanoutConfig struct {
	Discovery   *EndpointFanoutAdmin
	HTTPClient  *http.Client
	BearerToken string
	Timeout     time.Duration
	Concurrency int
}

// PodDrainFanout drains one backend Pod through every non-terminating proxy
// replica, including replicas that are temporarily not Ready but may still own
// live streams.
type PodDrainFanout struct {
	discovery   *EndpointFanoutAdmin
	client      *http.Client
	bearerToken string
	timeout     time.Duration
	concurrency int
	endpoints   func(context.Context, bool) ([]string, error)
}

// PodDrainResult is the all-replica drain acknowledgement.
type PodDrainResult struct {
	PodNamespace string `json:"pod_namespace"`
	PodName      string `json:"pod_name"`
	ProxyCount   int    `json:"proxy_count"`
	InFlight     int    `json:"in_flight"`
	State        string `json:"state"`
}

// NewPodDrainFanout constructs a drain broadcaster with redirect prevention
// and a total request deadline slightly longer than the downstream wait.
func NewPodDrainFanout(config PodDrainFanoutConfig) (*PodDrainFanout, error) {
	if config.Discovery == nil {
		return nil, errors.New("operator drain: proxy endpoint discovery is required")
	}
	if config.Timeout <= 0 || config.Timeout > time.Minute {
		return nil, errors.New("operator drain: timeout must be between zero and one minute")
	}
	if strings.TrimSpace(config.BearerToken) != config.BearerToken || strings.ContainsAny(config.BearerToken, "\r\n") {
		return nil, errors.New("operator drain: bearer token contains surrounding whitespace or a line break")
	}
	concurrency := config.Concurrency
	if concurrency == 0 {
		concurrency = defaultDrainFanoutConcurrency
	}
	if concurrency < 1 || concurrency > 64 {
		return nil, errors.New("operator drain: concurrency must be between 1 and 64")
	}
	client := &http.Client{}
	if config.HTTPClient != nil {
		*client = *config.HTTPClient
	}
	client.Timeout = config.Timeout + drainResponseGracePeriod
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &PodDrainFanout{
		discovery: config.Discovery, client: client, bearerToken: config.BearerToken,
		timeout: config.Timeout, concurrency: concurrency,
		endpoints: config.Discovery.endpoints,
	}, nil
}

// DrainPod waits for every known non-terminating proxy to report zero local
// in-flight streams for the Pod. A missing backend is an acknowledged zero.
func (fanout *PodDrainFanout) DrainPod(ctx context.Context, namespace, name string) (PodDrainResult, error) {
	result := PodDrainResult{PodNamespace: namespace, PodName: name, State: "draining"}
	if fanout == nil || fanout.discovery == nil || fanout.client == nil || fanout.endpoints == nil {
		return result, errors.New("operator drain: fanout is not configured")
	}
	if ctx == nil {
		return result, errors.New("operator drain: nil context")
	}
	if messages := validation.IsDNS1123Label(namespace); len(messages) != 0 {
		return result, errors.New("operator drain: invalid Pod namespace")
	}
	if messages := validation.IsDNS1123Subdomain(name); len(messages) != 0 {
		return result, errors.New("operator drain: invalid Pod name")
	}
	// Bound discovery and all worker batches together so a large replica set
	// cannot outlive the lifecycle hook's HTTP response and termination budget.
	ctx, cancel := context.WithTimeout(ctx, fanout.timeout+drainResponseGracePeriod)
	defer cancel()
	endpoints, err := fanout.endpoints(ctx, true)
	if err != nil {
		return result, err
	}
	result.ProxyCount = len(endpoints)
	if len(endpoints) == 0 {
		return result, errors.New("operator drain: proxy Service has no known endpoints")
	}
	type outcome struct {
		inFlight int
		err      error
	}
	outcomes := make([]outcome, len(endpoints))
	jobs := make(chan int)
	workers := fanout.concurrency
	if workers > len(endpoints) {
		workers = len(endpoints)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				outcomes[index].inFlight, outcomes[index].err = fanout.drainOne(ctx, endpoints[index], namespace, name)
			}
		}()
	}
	for index := range endpoints {
		jobs <- index
	}
	close(jobs)
	wait.Wait()
	failures := 0
	for _, outcome := range outcomes {
		if outcome.inFlight > 0 {
			result.InFlight += outcome.inFlight
		}
		if outcome.err != nil {
			failures++
		}
	}
	if failures != 0 || result.InFlight != 0 {
		return result, fmt.Errorf("operator drain: %d of %d proxy endpoints did not acknowledge zero in-flight streams", failures, len(endpoints))
	}
	result.State = "drained"
	return result, nil
}

func (fanout *PodDrainFanout) drainOne(
	ctx context.Context,
	endpoint string,
	namespace string,
	name string,
) (int, error) {
	path := "/internal/backends/by-pod/" + url.PathEscape(namespace) + "/" + url.PathEscape(name) + "/drain"
	target := strings.TrimRight(endpoint, "/") + path + "?timeout=" + url.QueryEscape(fanout.timeout.String())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
	if err != nil {
		return 0, errors.New("operator drain: create proxy request")
	}
	request.Header.Set("Accept", "application/json")
	if fanout.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+fanout.bearerToken)
	}
	response, err := fanout.client.Do(request)
	if err != nil {
		return 0, errors.New("operator drain: proxy request failed")
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxAdminResponseBytes+1))
	if err != nil || len(responseBody) > maxAdminResponseBytes {
		return 0, errors.New("operator drain: proxy response was unreadable or oversized")
	}
	if response.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	var decoded struct {
		PodNamespace string `json:"pod_namespace"`
		PodName      string `json:"pod_name"`
		InFlight     int    `json:"in_flight"`
	}
	if err := json.Unmarshal(responseBody, &decoded); err != nil || decoded.InFlight < 0 {
		return 0, errors.New("operator drain: proxy response was invalid")
	}
	if decoded.PodNamespace != namespace || decoded.PodName != name {
		return decoded.InFlight, errors.New("operator drain: proxy acknowledged a different Pod")
	}
	if response.StatusCode != http.StatusOK || decoded.InFlight != 0 {
		return decoded.InFlight, errors.New("operator drain: proxy did not reach zero in-flight streams")
	}
	return 0, nil
}
