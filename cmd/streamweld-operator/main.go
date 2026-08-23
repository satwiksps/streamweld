// Package main runs the Streamweld Kubernetes controller manager.
package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/satwiksps/streamweld/controllers"
	"github.com/satwiksps/streamweld/internal/apis/v1alpha1"
	"github.com/satwiksps/streamweld/internal/conformance"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

const maxCredentialFileBytes = 1 << 20

type operatorOptions struct {
	metricsAddress          string
	healthAddress           string
	leaderElection          bool
	adminURL                string
	adminTokenFile          string
	adminCAFile             string
	allowInsecureAdminHTTP  bool
	adminTimeout            time.Duration
	adminServiceNamespace   string
	adminServiceName        string
	adminEndpointPort       int
	adminFanoutConcurrency  int
	drainBindAddress        string
	drainTimeout            time.Duration
	drainFanoutConcurrency  int
	probeTimeout            time.Duration
	probeConcurrency        int
	maxConcurrentReconciles int
	enablePodMutation       bool
	webhookPort             int
	webhookCertDir          string
	drainHookHost           string
	drainHookPort           int
}

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(arguments []string, stderr io.Writer) int {
	options, help, err := parseOptions(arguments, stderr)
	if help {
		return 0
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	if err != nil {
		logger.Error("invalid operator configuration", "error", err)
		return 2
	}
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))

	token, err := readCredentialFile(options.adminTokenFile)
	if err != nil {
		logger.Error("admin credential could not be loaded", "error", publicCredentialError(err))
		return 2
	}
	adminURL, err := url.Parse(options.adminURL)
	if err != nil {
		logger.Error("proxy admin URL could not be parsed")
		return 2
	}
	adminHTTPClient, err := newAdminHTTPClient(options.adminCAFile, adminURL.Hostname())
	if err != nil {
		logger.Error("admin TLS configuration could not be loaded", "error", publicCredentialError(err))
		return 2
	}
	adminClientConfig := controllers.AdminClientConfig{
		BaseURL: options.adminURL, BearerToken: token, Timeout: options.adminTimeout,
		HTTPClient: adminHTTPClient, AllowInsecureHTTP: options.allowInsecureAdminHTTP,
		AllowUnauthenticatedLoopback: options.adminTokenFile == "",
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		logger.Error("register Kubernetes API scheme", "error", err)
		return 1
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		logger.Error("register Streamweld API scheme", "error", err)
		return 1
	}
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		logger.Error("load Kubernetes client configuration", "error", err)
		return 1
	}
	managerOptions := ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: options.metricsAddress},
		HealthProbeBindAddress:        options.healthAddress,
		LeaderElection:                options.leaderElection,
		LeaderElectionID:              "streamweld-operator.streamweld.io",
		LeaderElectionReleaseOnCancel: true,
	}
	if options.enablePodMutation {
		managerOptions.WebhookServer = webhook.NewServer(webhook.Options{
			Port: options.webhookPort, CertDir: options.webhookCertDir,
		})
	}
	manager, err := ctrl.NewManager(restConfig, managerOptions)
	if err != nil {
		logger.Error("create controller manager", "error", err)
		return 1
	}
	adminService := types.NamespacedName{Namespace: options.adminServiceNamespace, Name: options.adminServiceName}
	admin, err := controllers.NewEndpointFanoutAdmin(controllers.EndpointFanoutAdminConfig{
		Reader: manager.GetClient(), Service: adminService, EndpointPort: int32(options.adminEndpointPort),
		Client: adminClientConfig, Concurrency: options.adminFanoutConcurrency,
	})
	if err != nil {
		logger.Error("proxy admin fanout configuration was rejected", "error", err)
		return 2
	}
	drainFanout, err := controllers.NewPodDrainFanout(controllers.PodDrainFanoutConfig{
		Discovery: admin, HTTPClient: adminHTTPClient, BearerToken: token,
		Timeout: options.drainTimeout, Concurrency: options.drainFanoutConcurrency,
	})
	if err != nil {
		logger.Error("operator drain fanout configuration was rejected", "error", err)
		return 2
	}
	drainServer := &controllers.OperatorDrainServer{Address: options.drainBindAddress, Fanout: drainFanout}
	if err := manager.Add(drainServer); err != nil {
		logger.Error("register operator drain server", "error", err)
		return 1
	}

	probeClient := &http.Client{
		Timeout:       options.probeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	checker := conformance.NewChecker(probeClient, conformance.NewMemoryCache())
	reconciler := &controllers.InferenceRouteReconciler{
		Client: manager.GetClient(), Checker: checker, Admin: admin,
		ProbeTimeout: options.probeTimeout, ProbeConcurrency: options.probeConcurrency,
		MaxConcurrentReconciles: options.maxConcurrentReconciles, AdminService: adminService,
	}
	if err := reconciler.SetupWithManager(manager); err != nil {
		logger.Error("register InferenceRoute controller", "error", err)
		return 1
	}
	if options.enablePodMutation {
		mutator := &controllers.PodMutator{
			Client: manager.GetClient(), DrainHost: options.drainHookHost, DrainPort: int32(options.drainHookPort),
		}
		server := manager.GetWebhookServer()
		if err := controllers.RegisterPodMutationWebhook(server, true, mutator); err != nil {
			logger.Error("register pod mutation webhook", "error", err)
			return 1
		}
		if err := manager.AddReadyzCheck("webhook", server.StartedChecker()); err != nil {
			logger.Error("register webhook readiness check", "error", err)
			return 1
		}
	}
	if err := manager.AddHealthzCheck("ping", healthz.Ping); err != nil {
		logger.Error("register health check", "error", err)
		return 1
	}
	if err := manager.AddReadyzCheck("ping", healthz.Ping); err != nil {
		logger.Error("register readiness check", "error", err)
		return 1
	}
	if err := manager.AddReadyzCheck("drain", drainServer.Ready); err != nil {
		logger.Error("register drain readiness check", "error", err)
		return 1
	}
	logger.Info("starting Streamweld operator", "pod_mutation", options.enablePodMutation)
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error("controller manager stopped with an error", "error", err)
		return 1
	}
	return 0
}

func parseOptions(arguments []string, stderr io.Writer) (operatorOptions, bool, error) {
	options := operatorOptions{
		metricsAddress: ":8080", healthAddress: ":8081", leaderElection: true,
		adminURL: "https://streamweld-proxy:8080", adminTokenFile: "/var/run/secrets/streamweld/admin-token",
		adminTimeout: 5 * time.Second, adminServiceNamespace: "streamweld-system", adminServiceName: "streamweld-proxy",
		adminEndpointPort: 8080, adminFanoutConcurrency: 8,
		drainBindAddress: ":8082", drainTimeout: 10 * time.Second, drainFanoutConcurrency: 8,
		probeTimeout: 45 * time.Second, probeConcurrency: 4,
		maxConcurrentReconciles: 2, webhookPort: 9443,
		webhookCertDir: "/tmp/k8s-webhook-server/serving-certs",
		drainHookHost:  "streamweld-operator.streamweld-system.svc", drainHookPort: 8082,
	}
	flags := flag.NewFlagSet("streamweld-operator", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.metricsAddress, "metrics-bind-address", options.metricsAddress, "metrics listener address; 0 disables")
	flags.StringVar(&options.healthAddress, "health-probe-bind-address", options.healthAddress, "health and readiness listener address")
	flags.BoolVar(&options.leaderElection, "leader-elect", options.leaderElection, "enable Kubernetes leader election")
	flags.StringVar(&options.adminURL, "admin-url", options.adminURL, "proxy admin base URL")
	flags.StringVar(&options.adminTokenFile, "admin-token-file", options.adminTokenFile, "file containing the proxy admin bearer token")
	flags.StringVar(&options.adminCAFile, "admin-ca-file", options.adminCAFile, "optional PEM CA bundle for the proxy admin endpoint")
	flags.BoolVar(&options.allowInsecureAdminHTTP, "allow-insecure-admin-http", false, "allow plaintext admin HTTP; requires namespace isolation and NetworkPolicy")
	flags.DurationVar(&options.adminTimeout, "admin-timeout", options.adminTimeout, "deadline for one complete backend-set update")
	flags.StringVar(&options.adminServiceNamespace, "admin-service-namespace", options.adminServiceNamespace, "namespace containing the proxy Service and EndpointSlices")
	flags.StringVar(&options.adminServiceName, "admin-service-name", options.adminServiceName, "proxy Service name used for replica discovery")
	flags.IntVar(&options.adminEndpointPort, "admin-endpoint-port", options.adminEndpointPort, "proxy Pod admin listener port published in EndpointSlices")
	flags.IntVar(&options.adminFanoutConcurrency, "admin-fanout-concurrency", options.adminFanoutConcurrency, "maximum concurrent proxy replica updates")
	flags.StringVar(&options.drainBindAddress, "drain-bind-address", options.drainBindAddress, "in-cluster operator drain listener address")
	flags.DurationVar(&options.drainTimeout, "drain-timeout", options.drainTimeout, "maximum downstream proxy drain wait")
	flags.IntVar(&options.drainFanoutConcurrency, "drain-fanout-concurrency", options.drainFanoutConcurrency, "maximum concurrent proxy drain requests")
	flags.DurationVar(&options.probeTimeout, "probe-timeout", options.probeTimeout, "deadline for a backend conformance suite")
	flags.IntVar(&options.probeConcurrency, "probe-concurrency", options.probeConcurrency, "maximum concurrent conformance suites per route")
	flags.IntVar(&options.maxConcurrentReconciles, "max-concurrent-reconciles", options.maxConcurrentReconciles, "maximum concurrent route reconciliations")
	flags.BoolVar(&options.enablePodMutation, "enable-pod-mutation", false, "enable managed Pod preStop mutation (requires webhook TLS certificates)")
	flags.IntVar(&options.webhookPort, "webhook-port", options.webhookPort, "pod mutation webhook TLS port")
	flags.StringVar(&options.webhookCertDir, "webhook-cert-dir", options.webhookCertDir, "directory containing tls.crt and tls.key when pod mutation is enabled")
	flags.StringVar(&options.drainHookHost, "drain-hook-host", options.drainHookHost, "operator Service DNS name injected into the preStop HTTP hook")
	flags.IntVar(&options.drainHookPort, "drain-hook-port", options.drainHookPort, "operator Service drain port injected into the preStop HTTP hook")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: streamweld-operator [options]")
		flags.PrintDefaults()
	}
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return options, true, nil
		}
		return options, false, err
	}
	if flags.NArg() != 0 {
		return options, false, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if err := options.validate(); err != nil {
		return options, false, err
	}
	return options, false, nil
}

func (options operatorOptions) validate() error {
	var problems []error
	if options.adminTimeout <= 0 {
		problems = append(problems, errors.New("admin-timeout must be positive"))
	}
	if messages := validation.IsDNS1123Label(options.adminServiceNamespace); len(messages) != 0 {
		problems = append(problems, errors.New("admin-service-namespace must be a DNS label"))
	}
	if messages := validation.IsDNS1035Label(options.adminServiceName); len(messages) != 0 {
		problems = append(problems, errors.New("admin-service-name must be a DNS label"))
	}
	if options.adminEndpointPort < 1 || options.adminEndpointPort > 65535 {
		problems = append(problems, errors.New("admin-endpoint-port must be between 1 and 65535"))
	}
	if options.adminFanoutConcurrency < 1 || options.adminFanoutConcurrency > 64 {
		problems = append(problems, errors.New("admin-fanout-concurrency must be between 1 and 64"))
	}
	if strings.TrimSpace(options.drainBindAddress) == "" {
		problems = append(problems, errors.New("drain-bind-address is required"))
	}
	if options.drainTimeout <= 0 || options.drainTimeout > time.Minute {
		problems = append(problems, errors.New("drain-timeout must be positive and at most one minute"))
	}
	if options.drainFanoutConcurrency < 1 || options.drainFanoutConcurrency > 64 {
		problems = append(problems, errors.New("drain-fanout-concurrency must be between 1 and 64"))
	}
	if options.probeTimeout <= 0 {
		problems = append(problems, errors.New("probe-timeout must be positive"))
	}
	if options.probeConcurrency <= 0 {
		problems = append(problems, errors.New("probe-concurrency must be positive"))
	}
	if options.maxConcurrentReconciles <= 0 {
		problems = append(problems, errors.New("max-concurrent-reconciles must be positive"))
	}
	if options.enablePodMutation {
		if options.webhookPort < 1 || options.webhookPort > 65535 {
			problems = append(problems, errors.New("webhook-port must be between 1 and 65535"))
		}
		if strings.TrimSpace(options.webhookCertDir) == "" {
			problems = append(problems, errors.New("webhook-cert-dir is required when pod mutation is enabled"))
		}
		if options.drainHookPort < 1 || options.drainHookPort > 65535 {
			problems = append(problems, errors.New("drain-hook-port must be between 1 and 65535"))
		}
	}
	return errors.Join(problems...)
}

func readCredentialFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, maxCredentialFileBytes+1))
	if err != nil {
		return "", err
	}
	if len(content) > maxCredentialFileBytes {
		return "", errors.New("credential file exceeds size limit")
	}
	content = bytes.TrimSuffix(content, []byte("\n"))
	content = bytes.TrimSuffix(content, []byte("\r"))
	if len(content) == 0 {
		return "", errors.New("credential file is empty")
	}
	return string(content), nil
}

func newAdminHTTPClient(caFile, serverName string) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport is unavailable")
	}
	cloned := transport.Clone()
	tlsConfig := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	cloned.TLSClientConfig = tlsConfig
	if caFile == "" {
		return &http.Client{Transport: cloned}, nil
	}
	pem, err := readBoundedFile(caFile)
	if err != nil {
		return nil, err
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("admin CA file contains no certificates")
	}
	tlsConfig.RootCAs = roots
	return &http.Client{Transport: cloned}, nil
}

func readBoundedFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, maxCredentialFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(content) > maxCredentialFileBytes {
		return nil, errors.New("file exceeds size limit")
	}
	return content, nil
}

func publicCredentialError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New("configured credential file is unreadable or invalid")
}
