package chaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var kubernetesName = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$`)

const (
	backendOOMControlPort   = 8000
	backendOOMArmAction     = "arm"
	backendOOMTriggerAction = "trigger"
	backendOOMResetAction   = "reset"
)

// Injector applies and restores one scenario around an attached stream cohort.
type Injector interface {
	Prepare(context.Context, Scenario) error
	Inject(context.Context, Scenario) error
	Restore(context.Context, Scenario) error
}

// CommandRunner runs kubectl without a shell.
type CommandRunner interface {
	Run(context.Context, ...string) (string, error)
}

type execCommandRunner struct {
	binary string
}

func (runner execCommandRunner) Run(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, runner.binary, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", runner.binary, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

// KindConfig identifies only resources created by test/chaos/run-kind.sh.
type KindConfig struct {
	Namespace         string
	BackendDeployment string
	BackendContainer  string
	BackendSelector   string
	RedisDeployment   string
	InferenceRoute    string
	StableImage       string
	RolloutImage      string
	CommandTimeout    time.Duration
}

// KindInjector drives real Kubernetes disruptions through kubectl.
type KindInjector struct {
	config     KindConfig
	runner     CommandRunner
	originPod  string
	originNode string
}

// NewKindInjector validates resource identities before any kubectl command is run.
func NewKindInjector(config KindConfig, runner CommandRunner) (*KindInjector, error) {
	if config.CommandTimeout == 0 {
		config.CommandTimeout = 2 * time.Minute
	}
	if config.CommandTimeout <= 0 {
		return nil, errors.New("kind command timeout must be positive")
	}
	identities := map[string]string{
		"namespace": config.Namespace, "backend deployment": config.BackendDeployment,
		"backend container": config.BackendContainer, "redis deployment": config.RedisDeployment,
		"inference route": config.InferenceRoute,
	}
	for name, value := range identities {
		if !kubernetesName.MatchString(value) {
			return nil, fmt.Errorf("%s %q is not a canonical Kubernetes name", name, value)
		}
	}
	if config.BackendSelector == "" || strings.ContainsAny(config.BackendSelector, "\r\n\x00") {
		return nil, errors.New("backend selector is required and cannot contain control delimiters")
	}
	if config.StableImage == "" || config.RolloutImage == "" ||
		strings.ContainsAny(config.StableImage+config.RolloutImage, "\r\n\x00") {
		return nil, errors.New("stable and rollout backend images are required")
	}
	if runner == nil {
		runner = execCommandRunner{binary: "kubectl"}
	}
	return &KindInjector{config: config, runner: runner}, nil
}

// RequireCluster proves kubectl is connected; an enabled kind profile never skips.
func (injector *KindInjector) RequireCluster(ctx context.Context) error {
	_, err := injector.run(ctx, "cluster-info")
	if err != nil {
		return fmt.Errorf("kind chaos profile requires a reachable Kubernetes cluster: %w", err)
	}
	return nil
}

// Prepare establishes a deterministic origin cohort before clients attach.
func (injector *KindInjector) Prepare(ctx context.Context, scenario Scenario) error {
	injector.originPod = ""
	injector.originNode = ""
	switch scenario {
	case ScenarioPodKill, ScenarioRollingUpdate, ScenarioSpotReclaim, ScenarioBackendOOM, ScenarioUnsafe:
		if scenario == ScenarioUnsafe {
			if _, err := injector.run(ctx, "set", "env", "deployment/"+injector.config.BackendDeployment,
				"--namespace", injector.config.Namespace, "CHAOS_TEMPLATE_MODE=unsafe"); err != nil {
				return err
			}
		}
		if _, err := injector.run(ctx, "scale", "deployment/"+injector.config.BackendDeployment,
			"--namespace", injector.config.Namespace, "--replicas=1"); err != nil {
			return err
		}
		if err := injector.waitBackend(ctx); err != nil {
			return err
		}
		if err := injector.waitRouteBackends(ctx, 1); err != nil {
			return err
		}
		pod, err := injector.soleLiveBackendPod(ctx)
		if err != nil {
			return err
		}
		injector.originPod = pod
		if scenario == ScenarioBackendOOM {
			return injector.backendOOMControl(ctx, backendOOMArmAction)
		}
		if scenario == ScenarioSpotReclaim {
			node, err := injector.podNode(ctx, pod)
			if err != nil {
				return err
			}
			injector.originNode = node
		}
		if scenario == ScenarioUnsafe {
			_, err = injector.run(ctx, "wait", "inferenceroute/"+injector.config.InferenceRoute,
				"--namespace", injector.config.Namespace,
				"--for=jsonpath={.status.templateVerdict}=UNSAFE", "--timeout=120s")
			return err
		}
	case ScenarioClientDrop, ScenarioExplicitStop, ScenarioRedisDown, ScenarioSlowConsumer:
		return nil
	default:
		return fmt.Errorf("unsupported kind scenario %q", scenario)
	}
	return nil
}

// Inject performs the scenario only after every stream has produced a token.
func (injector *KindInjector) Inject(ctx context.Context, scenario Scenario) error {
	switch scenario {
	case ScenarioPodKill, ScenarioSpotReclaim, ScenarioBackendOOM, ScenarioUnsafe:
		if injector.originPod == "" {
			return errors.New("kind injection has no prepared origin Pod")
		}
		if _, err := injector.run(ctx, "scale", "deployment/"+injector.config.BackendDeployment,
			"--namespace", injector.config.Namespace, "--replicas=2"); err != nil {
			return err
		}
		if err := injector.waitBackend(ctx); err != nil {
			return err
		}
		if err := injector.waitRouteBackends(ctx, 2); err != nil {
			return err
		}
		if scenario == ScenarioBackendOOM {
			return injector.backendOOMControl(ctx, backendOOMTriggerAction)
		}
		if scenario == ScenarioSpotReclaim {
			if injector.originNode == "" {
				return errors.New("spot-reclaim injection has no prepared origin node")
			}
			if _, err := injector.run(ctx, "cordon", injector.originNode); err != nil {
				return err
			}
			_, err := injector.run(ctx, "drain", injector.originNode, "--ignore-daemonsets", "--delete-emptydir-data",
				"--force", "--grace-period=0", "--timeout=120s")
			return err
		}
		_, err := injector.run(ctx, "delete", "pod/"+injector.originPod, "--namespace", injector.config.Namespace,
			"--grace-period=0", "--force", "--wait=false")
		return err
	case ScenarioRollingUpdate:
		_, err := injector.run(ctx, "set", "image", "deployment/"+injector.config.BackendDeployment,
			injector.config.BackendContainer+"="+injector.config.RolloutImage, "--namespace", injector.config.Namespace)
		return err
	case ScenarioRedisDown:
		if _, err := injector.run(ctx, "scale", "deployment/"+injector.config.RedisDeployment,
			"--namespace", injector.config.Namespace, "--replicas=0"); err != nil {
			return err
		}
		_, err := injector.run(ctx, "wait", "pods", "--namespace", injector.config.Namespace,
			"--selector", "app.kubernetes.io/component=redis", "--for=delete", "--timeout=60s")
		return err
	case ScenarioClientDrop, ScenarioExplicitStop, ScenarioSlowConsumer:
		return nil
	default:
		return fmt.Errorf("unsupported kind scenario %q", scenario)
	}
}

// Restore returns the cluster to two safe deterministic backends and one Redis.
func (injector *KindInjector) Restore(ctx context.Context, scenario Scenario) error {
	var restoreErrors []error
	if scenario == ScenarioBackendOOM && injector.originPod != "" {
		if err := injector.backendOOMControl(ctx, backendOOMResetAction); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	if scenario == ScenarioSpotReclaim && injector.originNode != "" {
		if _, err := injector.run(ctx, "uncordon", injector.originNode); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	if scenario == ScenarioRedisDown {
		if _, err := injector.run(ctx, "scale", "deployment/"+injector.config.RedisDeployment,
			"--namespace", injector.config.Namespace, "--replicas=1"); err != nil {
			restoreErrors = append(restoreErrors, err)
		} else if _, err := injector.run(ctx, "rollout", "status", "deployment/"+injector.config.RedisDeployment,
			"--namespace", injector.config.Namespace, "--timeout=120s"); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	if scenario == ScenarioRollingUpdate {
		if _, err := injector.run(ctx, "set", "image", "deployment/"+injector.config.BackendDeployment,
			injector.config.BackendContainer+"="+injector.config.StableImage, "--namespace", injector.config.Namespace); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	if scenario == ScenarioUnsafe {
		if _, err := injector.run(ctx, "set", "env", "deployment/"+injector.config.BackendDeployment,
			"--namespace", injector.config.Namespace, "CHAOS_TEMPLATE_MODE=safe"); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	if scenario != ScenarioRedisDown {
		if _, err := injector.run(ctx, "scale", "deployment/"+injector.config.BackendDeployment,
			"--namespace", injector.config.Namespace, "--replicas=2"); err != nil {
			restoreErrors = append(restoreErrors, err)
		} else if err := injector.waitBackend(ctx); err != nil {
			restoreErrors = append(restoreErrors, err)
		} else if err := injector.waitRouteBackends(ctx, 2); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	if scenario == ScenarioUnsafe {
		if _, err := injector.run(ctx, "wait", "inferenceroute/"+injector.config.InferenceRoute,
			"--namespace", injector.config.Namespace,
			"--for=jsonpath={.status.templateVerdict}=SAFE", "--timeout=120s"); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	return errors.Join(restoreErrors...)
}

func (injector *KindInjector) backendOOMControl(ctx context.Context, action string) error {
	if injector.originPod == "" {
		return errors.New("backend-oom injection has no prepared origin Pod")
	}
	switch action {
	case backendOOMArmAction, backendOOMTriggerAction, backendOOMResetAction:
	default:
		return fmt.Errorf("unsupported backend OOM control action %q", action)
	}
	rawPath := fmt.Sprintf(
		"/api/v1/namespaces/%s/pods/%s:%d/proxy/_streamweld/test/backend-oom/%s",
		injector.config.Namespace,
		injector.originPod,
		backendOOMControlPort,
		action,
	)
	_, err := injector.run(ctx, "get", "--raw", rawPath)
	return err
}

func (injector *KindInjector) waitBackend(ctx context.Context) error {
	_, err := injector.run(ctx, "rollout", "status", "deployment/"+injector.config.BackendDeployment,
		"--namespace", injector.config.Namespace, "--timeout=120s")
	return err
}

func (injector *KindInjector) waitRouteBackends(ctx context.Context, count int) error {
	_, err := injector.run(ctx, "wait", "inferenceroute/"+injector.config.InferenceRoute,
		"--namespace", injector.config.Namespace,
		fmt.Sprintf("--for=jsonpath={.status.healthyBackends}=%d", count), "--timeout=120s")
	return err
}

func (injector *KindInjector) soleLiveBackendPod(ctx context.Context) (string, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		output, err := injector.run(ctx, "get", "pods", "--namespace", injector.config.Namespace,
			"--selector", injector.config.BackendSelector,
			"--field-selector=status.phase=Running", "--output=json")
		if err != nil {
			return "", err
		}
		var pods struct {
			Items []struct {
				Metadata struct {
					Name              string  `json:"name"`
					DeletionTimestamp *string `json:"deletionTimestamp"`
				} `json:"metadata"`
				Status struct {
					Phase      string `json:"phase"`
					Conditions []struct {
						Type   string `json:"type"`
						Status string `json:"status"`
					} `json:"conditions"`
				} `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal([]byte(output), &pods); err != nil {
			return "", fmt.Errorf("decode backend Pods: %w", err)
		}
		livePods := make([]string, 0, len(pods.Items))
		readyPods := make([]string, 0, len(pods.Items))
		for _, pod := range pods.Items {
			if pod.Metadata.DeletionTimestamp != nil {
				continue
			}
			livePods = append(livePods, pod.Metadata.Name)
			for _, condition := range pod.Status.Conditions {
				if pod.Status.Phase == "Running" && condition.Type == "Ready" && condition.Status == "True" {
					readyPods = append(readyPods, pod.Metadata.Name)
					break
				}
			}
		}
		if len(livePods) == 1 && len(readyPods) == 1 {
			if !kubernetesName.MatchString(readyPods[0]) {
				return "", fmt.Errorf("kubectl returned invalid backend Pod %q", readyPods[0])
			}
			return readyPods[0], nil
		}
		state := fmt.Sprintf("%d non-terminating and %d Ready", len(livePods), len(readyPods))
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for exactly one live Ready backend Pod (%s): %w", state, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (injector *KindInjector) podNode(ctx context.Context, pod string) (string, error) {
	output, err := injector.run(ctx, "get", "pod/"+pod, "--namespace", injector.config.Namespace,
		"--output=jsonpath={.spec.nodeName}")
	if err != nil {
		return "", err
	}
	if !kubernetesName.MatchString(output) {
		return "", fmt.Errorf("kubectl returned invalid node %q", output)
	}
	return output, nil
}

func (injector *KindInjector) run(ctx context.Context, args ...string) (string, error) {
	commandContext, cancel := context.WithTimeout(ctx, injector.config.CommandTimeout)
	defer cancel()
	return injector.runner.Run(commandContext, args...)
}
