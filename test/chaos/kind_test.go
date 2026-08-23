package chaos

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingRunner struct {
	mu           sync.Mutex
	commands     [][]string
	podLists     []string
	podListCalls int
}

func (runner *recordingRunner) Run(_ context.Context, args ...string) (string, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.commands = append(runner.commands, append([]string(nil), args...))
	joined := strings.Join(args, " ")
	switch {
	case strings.Contains(joined, "jsonpath={.spec.nodeName}"):
		return "streamweld-chaos-worker", nil
	case strings.Contains(joined, "get pods") && strings.Contains(joined, "--output=json"):
		if len(runner.podLists) != 0 {
			index := min(runner.podListCalls, len(runner.podLists)-1)
			runner.podListCalls++
			return runner.podLists[index], nil
		}
		return `{"items":[{
			"metadata":{"name":"streamweld-chaos-backend-origin"},
			"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}
		}]}`, nil
	default:
		return "ok", nil
	}
}

func TestKindInjectorSelectsTheSurvivingScaleDownPod(t *testing.T) {
	t.Parallel()

	ready := `"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}`
	runner := &recordingRunner{podLists: []string{
		`{"items":[` +
			`{"metadata":{"name":"streamweld-chaos-backend-terminating"},` + ready + `},` +
			`{"metadata":{"name":"streamweld-chaos-backend-survivor"},` + ready + `}` +
			`]}`,
		`{"items":[` +
			`{"metadata":{"name":"streamweld-chaos-backend-terminating",` +
			`"deletionTimestamp":"2026-08-23T04:31:26Z"},` + ready + `},` +
			`{"metadata":{"name":"streamweld-chaos-backend-survivor"},` + ready + `}` +
			`]}`,
	}}
	injector, err := NewKindInjector(testKindConfig(), runner)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := injector.Prepare(ctx, ScenarioPodKill); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := injector.Inject(ctx, ScenarioPodKill); err != nil {
		t.Fatalf("Inject() error = %v", err)
	}
	if !runner.contains("delete", "pod/streamweld-chaos-backend-survivor") {
		t.Fatalf("commands do not delete the surviving Pod: %#v", runner.commands)
	}
	if runner.contains("delete", "pod/streamweld-chaos-backend-terminating") {
		t.Fatalf("commands delete the already-terminating Pod: %#v", runner.commands)
	}
	if runner.podListCalls < 2 {
		t.Fatalf("Pod selection did not wait for scale-down convergence: %d list calls", runner.podListCalls)
	}
}

func (runner *recordingRunner) contains(parts ...string) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	for _, command := range runner.commands {
		joined := strings.Join(command, " ")
		matched := true
		for _, part := range parts {
			if !strings.Contains(joined, part) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func TestKindInjectorUsesDistinctPhysicalInjections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario Scenario
		want     [][]string
	}{
		{ScenarioPodKill, [][]string{{"scale", "--replicas=1"}, {"scale", "--replicas=2"}, {"delete", "--force"}}},
		{ScenarioRollingUpdate, [][]string{{"scale", "--replicas=1"}, {"set image", "kind-rollout"}, {"set image", "backend:kind"}}},
		{ScenarioSpotReclaim, [][]string{{"cordon", "chaos-worker"}, {"drain", "--grace-period=0"}, {"uncordon", "chaos-worker"}}},
		{ScenarioBackendOOM, nil},
		{ScenarioClientDrop, nil},
		{ScenarioExplicitStop, nil},
		{ScenarioRedisDown, [][]string{{"streamweld-redis", "--replicas=0"}, {"streamweld-redis", "--replicas=1"}}},
		{ScenarioSlowConsumer, nil},
		{ScenarioUnsafe, [][]string{{"CHAOS_TEMPLATE_MODE=unsafe"}, {"templateVerdict}=UNSAFE"}, {"delete", "--force"}, {"CHAOS_TEMPLATE_MODE=safe"}}},
	}
	for _, test := range tests {
		test := test
		t.Run(string(test.scenario), func(t *testing.T) {
			t.Parallel()
			runner := &recordingRunner{}
			injector, err := NewKindInjector(testKindConfig(), runner)
			if err != nil {
				t.Fatalf("NewKindInjector() error = %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := injector.Prepare(ctx, test.scenario); err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			if err := injector.Inject(ctx, test.scenario); err != nil {
				t.Fatalf("Inject() error = %v", err)
			}
			if err := injector.Restore(ctx, test.scenario); err != nil {
				t.Fatalf("Restore() error = %v", err)
			}
			for _, parts := range test.want {
				if !runner.contains(parts...) {
					t.Errorf("commands do not contain %q: %#v", parts, runner.commands)
				}
			}
		})
	}
}

func TestKindInjectorRejectsUnsafeResourceNames(t *testing.T) {
	t.Parallel()

	config := testKindConfig()
	config.Namespace = "../../default"
	if _, err := NewKindInjector(config, &recordingRunner{}); err == nil {
		t.Fatal("NewKindInjector() accepted an unsafe namespace")
	}
}

func TestKindInjectorSurfacesCommandFailure(t *testing.T) {
	t.Parallel()

	runner := failingRunner{}
	injector, err := NewKindInjector(testKindConfig(), runner)
	if err != nil {
		t.Fatal(err)
	}
	if err := injector.RequireCluster(context.Background()); err == nil || !strings.Contains(err.Error(), "requires a reachable") {
		t.Fatalf("RequireCluster() error = %v", err)
	}
}

type failingRunner struct{}

func (failingRunner) Run(context.Context, ...string) (string, error) {
	return "", fmt.Errorf("injected kubectl failure")
}

func testKindConfig() KindConfig {
	return KindConfig{
		Namespace:         "streamweld-system",
		BackendDeployment: "streamweld-chaos-backend",
		BackendContainer:  "backend",
		BackendSelector:   "app.kubernetes.io/name=streamweld-chaos-backend",
		RedisDeployment:   "streamweld-redis",
		InferenceRoute:    "deterministic-chaos",
		StableImage:       "streamweld-chaos-backend:kind",
		RolloutImage:      "streamweld-chaos-backend:kind-rollout",
	}
}
