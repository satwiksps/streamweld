package controllers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"sort"
	"strconv"
	"sync"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultAdminFanoutConcurrency = 8

// EndpointFanoutAdminConfig discovers every Ready replica behind one proxy
// Service and sends each the same complete route snapshot.
type EndpointFanoutAdminConfig struct {
	Reader       client.Reader
	Service      types.NamespacedName
	EndpointPort int32
	Client       AdminClientConfig
	Concurrency  int
}

// EndpointFanoutAdmin prevents a ClusterIP load balancer from updating only
// one proxy process in a multi-replica deployment.
type EndpointFanoutAdmin struct {
	reader       client.Reader
	service      types.NamespacedName
	endpointPort int32
	baseURL      *url.URL
	clientConfig AdminClientConfig
	concurrency  int
	newClient    func(string) (BackendAdmin, error)
}

// NewEndpointFanoutAdmin validates discovery and transport configuration.
func NewEndpointFanoutAdmin(config EndpointFanoutAdminConfig) (*EndpointFanoutAdmin, error) {
	if config.Reader == nil {
		return nil, fmt.Errorf("%w: Kubernetes reader is required for admin fanout", ErrInvalidAdminConfig)
	}
	if problems := validation.IsDNS1123Label(config.Service.Namespace); len(problems) != 0 {
		return nil, fmt.Errorf("%w: invalid admin Service namespace", ErrInvalidAdminConfig)
	}
	if problems := validation.IsDNS1035Label(config.Service.Name); len(problems) != 0 {
		return nil, fmt.Errorf("%w: invalid admin Service name", ErrInvalidAdminConfig)
	}
	if config.EndpointPort < 1 || config.EndpointPort > 65535 {
		return nil, fmt.Errorf("%w: admin endpoint port must be between 1 and 65535", ErrInvalidAdminConfig)
	}
	baseURL, err := parseAdminBaseURL(config.Client.BaseURL, config.Client.AllowInsecureHTTP)
	if err != nil {
		return nil, err
	}
	concurrency := config.Concurrency
	if concurrency == 0 {
		concurrency = defaultAdminFanoutConcurrency
	}
	if concurrency < 1 || concurrency > 64 {
		return nil, fmt.Errorf("%w: admin fanout concurrency must be between 1 and 64", ErrInvalidAdminConfig)
	}
	admin := &EndpointFanoutAdmin{
		reader: config.Reader, service: config.Service, endpointPort: config.EndpointPort,
		baseURL: baseURL, clientConfig: config.Client, concurrency: concurrency,
	}
	admin.newClient = func(endpoint string) (BackendAdmin, error) {
		clientConfig := admin.clientConfig
		clientConfig.BaseURL = endpoint
		return NewHTTPAdminClient(clientConfig)
	}
	return admin, nil
}

// Apply broadcasts a snapshot and succeeds only after every Ready replica has
// acknowledged it. Serving backend counts must agree. Active streams are
// replica-local and summed; draining backend identities are unioned so the
// same retained backend is not counted once per replica. Legacy count-only
// replies contribute a conservative maximum instead of requiring equality.
func (admin *EndpointFanoutAdmin) Apply(
	ctx context.Context,
	route types.NamespacedName,
	snapshot BackendSnapshot,
) (AdminResult, error) {
	if admin == nil || admin.reader == nil || admin.newClient == nil {
		return AdminResult{}, fmt.Errorf("%w: nil fanout client", ErrInvalidAdminConfig)
	}
	endpoints, err := admin.endpoints(ctx, snapshot.Deleted)
	if err != nil {
		return AdminResult{}, err
	}
	if len(endpoints) == 0 && !snapshot.Deleted {
		return AdminResult{}, errors.New("operator admin: proxy Service has no Ready endpoints")
	}
	if len(endpoints) == 0 {
		return AdminResult{
			Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
		}, nil
	}
	type outcome struct {
		result AdminResult
		err    error
	}
	outcomes := make([]outcome, len(endpoints))
	jobs := make(chan int)
	workers := admin.concurrency
	if workers > len(endpoints) {
		workers = len(endpoints)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				client, createErr := admin.newClient(endpoints[index])
				if createErr != nil {
					outcomes[index].err = createErr
					continue
				}
				outcomes[index].result, outcomes[index].err = client.Apply(ctx, route, snapshot)
			}
		}()
	}
	for index := range endpoints {
		jobs <- index
	}
	close(jobs)
	wait.Wait()

	aggregate := AdminResult{
		Route: route.String(), Model: snapshot.Model, AppliedGeneration: snapshot.ObservedGeneration,
	}
	drainingIDs := make(map[string]struct{})
	completeDrainingIDUnion := true
	var fallbackDrainingCount int32
	failures := 0
	initialized := false
	for _, outcome := range outcomes {
		if outcome.err != nil {
			failures++
			continue
		}
		result := outcome.result
		if result.Route != route.String() || result.Model != snapshot.Model || result.AppliedGeneration < snapshot.ObservedGeneration {
			failures++
			continue
		}
		if result.BackendCount < 0 || result.DrainingBackends < 0 || result.ActiveStreams < 0 ||
			validateDrainingBackendIDs(result) != nil {
			failures++
			continue
		}
		if !initialized {
			aggregate.BackendCount = result.BackendCount
			aggregate.AppliedGeneration = result.AppliedGeneration
			initialized = true
		} else if result.BackendCount != aggregate.BackendCount || result.AppliedGeneration != aggregate.AppliedGeneration {
			failures++
			continue
		}
		if result.DrainingBackends > fallbackDrainingCount {
			fallbackDrainingCount = result.DrainingBackends
		}
		if result.DrainingBackends > 0 && len(result.DrainingBackendIDs) == 0 {
			completeDrainingIDUnion = false
		}
		for _, id := range result.DrainingBackendIDs {
			if _, exists := drainingIDs[id]; exists {
				continue
			}
			if len(drainingIDs) == maxAdminSnapshotBackends {
				completeDrainingIDUnion = false
				continue
			}
			drainingIDs[id] = struct{}{}
		}
		if result.ActiveStreams > math.MaxInt64-aggregate.ActiveStreams {
			failures++
			continue
		}
		aggregate.ActiveStreams += result.ActiveStreams
	}
	if failures != 0 {
		return aggregate, fmt.Errorf("operator admin: %d of %d proxy endpoints failed to acknowledge the snapshot", failures, len(endpoints))
	}
	aggregate.DrainingBackends = int32(len(drainingIDs))
	if fallbackDrainingCount > aggregate.DrainingBackends {
		aggregate.DrainingBackends = fallbackDrainingCount
	}
	if completeDrainingIDUnion && len(drainingIDs) <= maxAdminDrainingBackendIDs &&
		int32(len(drainingIDs)) == aggregate.DrainingBackends {
		aggregate.DrainingBackendIDs = make([]string, 0, len(drainingIDs))
		for id := range drainingIDs {
			aggregate.DrainingBackendIDs = append(aggregate.DrainingBackendIDs, id)
		}
		sort.Strings(aggregate.DrainingBackendIDs)
	}
	return aggregate, nil
}

func (admin *EndpointFanoutAdmin) endpoints(ctx context.Context, includeNotReady bool) ([]string, error) {
	slices := &discoveryv1.EndpointSliceList{}
	if err := admin.reader.List(
		ctx,
		slices,
		client.InNamespace(admin.service.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: admin.service.Name},
	); err != nil {
		return nil, errors.New("operator admin: list proxy EndpointSlices")
	}
	replicas := make(map[string]string)
	for sliceIndex := range slices.Items {
		slice := &slices.Items[sliceIndex]
		if !endpointSliceHasPort(slice, admin.endpointPort) {
			continue
		}
		for endpointIndex := range slice.Endpoints {
			endpoint := &slice.Endpoints[endpointIndex]
			if (!includeNotReady && (endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready)) ||
				(endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating) {
				continue
			}
			for _, address := range endpoint.Addresses {
				validAddress, ok := preferredEndpointAddress(slice.AddressType, []string{address})
				if !ok {
					continue
				}
				endpointURL := *admin.baseURL
				endpointURL.Host = net.JoinHostPort(validAddress, strconv.Itoa(int(admin.endpointPort)))
				endpointURL.RawPath = ""
				urlString := endpointURL.String()
				replicaKey := "address:" + urlString
				if endpoint.TargetRef != nil && endpoint.TargetRef.Kind == "Pod" && endpoint.TargetRef.UID != "" {
					replicaKey = "pod-uid:" + string(endpoint.TargetRef.UID)
				}
				if current, exists := replicas[replicaKey]; !exists || urlString < current {
					replicas[replicaKey] = urlString
				}
			}
		}
	}
	result := make([]string, 0, len(replicas))
	for _, endpoint := range replicas {
		result = append(result, endpoint)
	}
	sort.Strings(result)
	return result, nil
}
