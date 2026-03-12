package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/conformance"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultAdminTimeout      = 5 * time.Second
	maxAdminResponseBytes    = 64 << 10
	maxAdminRequestBytes     = 1 << 20
	maxAdminSnapshotBackends = 4096
	// Keep a complete identity list only while it fits comfortably within the
	// bounded admin response. Larger/legacy replies fall back to their count.
	maxAdminDrainingBackendIDs = 32
	maxAdminBackendIDBytes     = 512
)

var (
	// ErrStaleSnapshot means the proxy already accepted this or a newer route generation.
	ErrStaleSnapshot = errors.New("operator admin: stale backend snapshot")
	// ErrInvalidAdminConfig means the admin endpoint or credentials are unsafe or malformed.
	ErrInvalidAdminConfig = errors.New("operator admin: invalid configuration")
	// ErrInvalidSnapshot means a route backend snapshot cannot be sent safely.
	ErrInvalidSnapshot = errors.New("operator admin: invalid backend snapshot")
)

// BackendRegistration is one backend in a complete route snapshot.
type BackendRegistration struct {
	ID              string              `json:"id"`
	URL             string              `json:"url"`
	ModelVersion    string              `json:"model_version,omitempty"`
	TemplateVerdict conformance.Verdict `json:"template_verdict"`
	PodNamespace    string              `json:"pod_namespace,omitempty"`
	PodName         string              `json:"pod_name,omitempty"`
}

// BackendSnapshot replaces the proxy's complete backend set for one route.
type BackendSnapshot struct {
	Model              string                `json:"model"`
	ObservedGeneration int64                 `json:"observed_generation"`
	UID                string                `json:"uid"`
	Deleted            bool                  `json:"deleted"`
	Policy             AdminRoutePolicy      `json:"policy"`
	Backends           []BackendRegistration `json:"backends"`
}

// AdminRoutePolicy is the fully materialized per-route durability policy sent
// to the proxy with every complete backend snapshot.
type AdminRoutePolicy struct {
	MaxMigrations         int32  `json:"max_migrations"`
	MaxMigrationTokens    int64  `json:"max_migration_tokens"`
	MaxStreamDuration     string `json:"max_stream_duration"`
	OrphanPolicy          string `json:"orphan_policy"`
	OrphanTimeout         string `json:"orphan_timeout"`
	AllowCrossVersion     bool   `json:"allow_cross_version"`
	AllowStructuredResume bool   `json:"allow_structured_resume"`
	SeamWindowBytes       int32  `json:"seam_window_bytes"`
	JournalTTL            string `json:"journal_ttl"`
}

// AdminResult is the proxy's authoritative state after applying a snapshot.
// DrainingBackendIDs, when non-empty, is the complete bounded, sorted identity
// set represented by DrainingBackends. Its omission preserves compatibility
// with older proxies and count-only fallback for unusually large sets.
type AdminResult struct {
	Route              string   `json:"route"`
	Model              string   `json:"model"`
	AppliedGeneration  int64    `json:"applied_generation"`
	BackendCount       int32    `json:"backend_count"`
	DrainingBackends   int32    `json:"draining_backends"`
	DrainingBackendIDs []string `json:"draining_backend_ids,omitempty"`
	ActiveStreams      int64    `json:"active_streams"`
}

// BackendAdmin applies complete, generation-fenced route snapshots.
type BackendAdmin interface {
	Apply(context.Context, types.NamespacedName, BackendSnapshot) (AdminResult, error)
}

// AdminClientConfig configures a hardened proxy admin client.
type AdminClientConfig struct {
	BaseURL                      string
	BearerToken                  string
	Timeout                      time.Duration
	HTTPClient                   *http.Client
	AllowInsecureHTTP            bool
	AllowUnauthenticatedLoopback bool
}

// HTTPAdminClient sends bounded, authenticated route updates to a proxy.
type HTTPAdminClient struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

// NewHTTPAdminClient validates configuration before constructing a client.
func NewHTTPAdminClient(config AdminClientConfig) (*HTTPAdminClient, error) {
	baseURL, err := parseAdminBaseURL(config.BaseURL, config.AllowInsecureHTTP)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(config.BearerToken)
	if token != config.BearerToken || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("%w: bearer token contains surrounding whitespace or a line break", ErrInvalidAdminConfig)
	}
	if token == "" && (!config.AllowUnauthenticatedLoopback || !isLoopbackHost(baseURL.Hostname())) {
		return nil, fmt.Errorf("%w: bearer token is required", ErrInvalidAdminConfig)
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = defaultAdminTimeout
	}
	if timeout < 0 {
		return nil, fmt.Errorf("%w: timeout must be positive", ErrInvalidAdminConfig)
	}
	client := &http.Client{}
	if config.HTTPClient != nil {
		*client = *config.HTTPClient
	}
	client.Timeout = timeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &HTTPAdminClient{baseURL: baseURL, token: token, client: client}, nil
}

// Apply PUTs one deterministic complete snapshot. A 409 returns ErrStaleSnapshot
// and the proxy's current counters so callers can distinguish an exact retry
// from an update that lost the generation fence.
func (client *HTTPAdminClient) Apply(
	ctx context.Context,
	route types.NamespacedName,
	snapshot BackendSnapshot,
) (AdminResult, error) {
	if client == nil || client.baseURL == nil || client.client == nil {
		return AdminResult{}, fmt.Errorf("%w: nil client", ErrInvalidAdminConfig)
	}
	if ctx == nil {
		return AdminResult{}, fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := validateRoute(route); err != nil {
		return AdminResult{}, err
	}
	normalized, err := normalizeSnapshot(snapshot)
	if err != nil {
		return AdminResult{}, err
	}
	body, err := json.Marshal(normalized)
	if err != nil {
		return AdminResult{}, fmt.Errorf("operator admin: encode backend snapshot: %w", err)
	}
	if len(body) > maxAdminRequestBytes {
		return AdminResult{}, fmt.Errorf("%w: encoded snapshot exceeds %d bytes", ErrInvalidSnapshot, maxAdminRequestBytes)
	}

	endpoint := strings.TrimRight(client.baseURL.String(), "/") +
		"/internal/routes/" + url.PathEscape(route.String()) + "/backends"
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return AdminResult{}, fmt.Errorf("operator admin: create request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	if client.token != "" {
		request.Header.Set("Authorization", "Bearer "+client.token)
	}

	response, err := client.client.Do(request)
	if err != nil {
		return AdminResult{}, fmt.Errorf("operator admin: apply backend snapshot: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxAdminResponseBytes+1))
	if err != nil {
		return AdminResult{}, fmt.Errorf("operator admin: read response: %w", err)
	}
	if len(responseBody) > maxAdminResponseBytes {
		return AdminResult{}, fmt.Errorf("operator admin: response exceeds %d bytes", maxAdminResponseBytes)
	}
	var result AdminResult
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return AdminResult{}, fmt.Errorf("operator admin: decode %s response", response.Status)
	}
	if response.StatusCode == http.StatusConflict {
		return result, fmt.Errorf("%w: proxy generation %d", ErrStaleSnapshot, result.AppliedGeneration)
	}
	if response.StatusCode != http.StatusOK {
		return AdminResult{}, fmt.Errorf("operator admin: proxy returned %s", response.Status)
	}
	if result.Route != route.String() || result.Model != normalized.Model || result.AppliedGeneration < normalized.ObservedGeneration {
		return AdminResult{}, errors.New("operator admin: proxy returned an inconsistent acknowledgement")
	}
	if result.BackendCount < 0 || result.DrainingBackends < 0 || result.ActiveStreams < 0 {
		return AdminResult{}, errors.New("operator admin: proxy returned negative counters")
	}
	if err := validateDrainingBackendIDs(result); err != nil {
		return AdminResult{}, err
	}
	return result, nil
}

func validateDrainingBackendIDs(result AdminResult) error {
	if len(result.DrainingBackendIDs) > maxAdminDrainingBackendIDs {
		return errors.New("operator admin: proxy returned too many draining backend identities")
	}
	if len(result.DrainingBackendIDs) == 0 {
		// An omitted list is the compatibility representation used by older
		// proxies and by a proxy whose complete identity set exceeds the cap.
		return nil
	}
	if int64(len(result.DrainingBackendIDs)) != int64(result.DrainingBackends) {
		return errors.New("operator admin: proxy returned an incomplete draining backend identity set")
	}
	for index, id := range result.DrainingBackendIDs {
		if len(id) > maxAdminBackendIDBytes || backend.ID(id).Validate() != nil {
			return errors.New("operator admin: proxy returned an invalid draining backend identity")
		}
		if index > 0 && result.DrainingBackendIDs[index-1] >= id {
			return errors.New("operator admin: proxy returned noncanonical draining backend identities")
		}
	}
	return nil
}

func parseAdminBaseURL(raw string, allowInsecureHTTP bool) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, fmt.Errorf("%w: admin URL is required and must not be padded", ErrInvalidAdminConfig)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: parse admin URL", ErrInvalidAdminConfig)
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: admin URL must be an absolute URL without credentials, query, or fragment", ErrInvalidAdminConfig)
	}
	if parsed.Scheme != "https" {
		if parsed.Scheme != "http" || !allowInsecureHTTP {
			return nil, fmt.Errorf("%w: admin URL must use HTTPS unless plaintext HTTP is explicitly enabled", ErrInvalidAdminConfig)
		}
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateRoute(route types.NamespacedName) error {
	if problems := validation.IsDNS1123Label(route.Namespace); len(problems) != 0 {
		return fmt.Errorf("%w: invalid route namespace", ErrInvalidSnapshot)
	}
	if problems := validation.IsDNS1123Subdomain(route.Name); len(problems) != 0 {
		return fmt.Errorf("%w: invalid route name", ErrInvalidSnapshot)
	}
	return nil
}

func normalizeSnapshot(snapshot BackendSnapshot) (BackendSnapshot, error) {
	if strings.TrimSpace(snapshot.Model) != snapshot.Model || snapshot.Model == "" {
		return BackendSnapshot{}, fmt.Errorf("%w: model is required and must not be padded", ErrInvalidSnapshot)
	}
	if snapshot.ObservedGeneration <= 0 {
		return BackendSnapshot{}, fmt.Errorf("%w: observed generation must be positive", ErrInvalidSnapshot)
	}
	if snapshot.UID == "" || len(snapshot.UID) > 128 || strings.TrimSpace(snapshot.UID) != snapshot.UID || strings.ContainsAny(snapshot.UID, "\r\n\x00") {
		return BackendSnapshot{}, fmt.Errorf("%w: route UID is required and must be unpadded", ErrInvalidSnapshot)
	}
	if len(snapshot.Model) > 512 {
		return BackendSnapshot{}, fmt.Errorf("%w: model exceeds 512 bytes", ErrInvalidSnapshot)
	}
	if snapshot.Deleted && len(snapshot.Backends) != 0 {
		return BackendSnapshot{}, fmt.Errorf("%w: a deleted route snapshot must have no backends", ErrInvalidSnapshot)
	}
	if err := validateAdminPolicy(snapshot.Policy); err != nil {
		return BackendSnapshot{}, err
	}
	if len(snapshot.Backends) > maxAdminSnapshotBackends {
		return BackendSnapshot{}, fmt.Errorf("%w: backend count exceeds %d", ErrInvalidSnapshot, maxAdminSnapshotBackends)
	}
	snapshot.Backends = append([]BackendRegistration(nil), snapshot.Backends...)
	seen := make(map[string]struct{}, len(snapshot.Backends))
	for index := range snapshot.Backends {
		candidate := &snapshot.Backends[index]
		if len(candidate.ID) > maxAdminBackendIDBytes || backend.ID(candidate.ID).Validate() != nil {
			return BackendSnapshot{}, fmt.Errorf("%w: backend %d has invalid ID", ErrInvalidSnapshot, index)
		}
		if _, exists := seen[candidate.ID]; exists {
			return BackendSnapshot{}, fmt.Errorf("%w: duplicate backend ID %q", ErrInvalidSnapshot, candidate.ID)
		}
		seen[candidate.ID] = struct{}{}
		parsed, err := url.Parse(candidate.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
			parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return BackendSnapshot{}, fmt.Errorf("%w: backend %q has an invalid URL", ErrInvalidSnapshot, candidate.ID)
		}
		if candidate.TemplateVerdict != conformance.VerdictSafe &&
			candidate.TemplateVerdict != conformance.VerdictDegraded &&
			candidate.TemplateVerdict != conformance.VerdictUnsafe {
			return BackendSnapshot{}, fmt.Errorf("%w: backend %q has no completed template verdict", ErrInvalidSnapshot, candidate.ID)
		}
		if (candidate.PodNamespace == "") != (candidate.PodName == "") {
			return BackendSnapshot{}, fmt.Errorf("%w: backend %q has incomplete pod identity", ErrInvalidSnapshot, candidate.ID)
		}
		if candidate.PodNamespace != "" {
			if problems := validation.IsDNS1123Label(candidate.PodNamespace); len(problems) != 0 {
				return BackendSnapshot{}, fmt.Errorf("%w: backend %q has invalid pod namespace", ErrInvalidSnapshot, candidate.ID)
			}
			if problems := validation.IsDNS1123Subdomain(candidate.PodName); len(problems) != 0 {
				return BackendSnapshot{}, fmt.Errorf("%w: backend %q has invalid pod name", ErrInvalidSnapshot, candidate.ID)
			}
		}
	}
	sort.Slice(snapshot.Backends, func(left, right int) bool {
		if snapshot.Backends[left].ID != snapshot.Backends[right].ID {
			return snapshot.Backends[left].ID < snapshot.Backends[right].ID
		}
		return snapshot.Backends[left].URL < snapshot.Backends[right].URL
	})
	return snapshot, nil
}

func validateAdminPolicy(policy AdminRoutePolicy) error {
	if policy.MaxMigrations < 0 || policy.MaxMigrationTokens < 0 || policy.SeamWindowBytes <= 0 {
		return fmt.Errorf("%w: policy contains an invalid numeric limit", ErrInvalidSnapshot)
	}
	if policy.OrphanPolicy != "continue" && policy.OrphanPolicy != "cancel_after" && policy.OrphanPolicy != "cancel" {
		return fmt.Errorf("%w: policy contains an invalid orphan policy", ErrInvalidSnapshot)
	}
	for field, value := range map[string]string{
		"max stream duration": policy.MaxStreamDuration,
		"orphan timeout":      policy.OrphanTimeout,
		"journal TTL":         policy.JournalTTL,
	} {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return fmt.Errorf("%w: policy %s must be a positive duration", ErrInvalidSnapshot, field)
		}
	}
	return nil
}
