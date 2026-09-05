package streamweld_test

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestRenderedDeploymentRolloutsRespectJournalStorage(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("Helm is required to exercise chart rendering; CI installs it for this test")
	}
	tests := []struct {
		name       string
		values     string
		deployment string
		strategy   appsv1.DeploymentStrategyType
	}{
		{"memory proxy", "journal.backend=memory", "streamweld-proxy", appsv1.RecreateDeploymentStrategyType},
		{"shared proxy", "journal.backend=redis,redis.enabled=true", "streamweld-proxy", appsv1.RollingUpdateDeploymentStrategyType},
		{"ephemeral Redis", "journal.backend=redis,redis.enabled=true", "streamweld-redis", appsv1.RecreateDeploymentStrategyType},
		{"persistent Redis", "journal.backend=redis,redis.enabled=true,redis.persistence.enabled=true", "streamweld-redis", appsv1.RecreateDeploymentStrategyType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, helm, "template", "streamweld", ".", "--namespace", "streamweld-system", "--kube-version", "1.32.0", "--set", test.values)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v\n%s", err, output)
			}
			decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(string(output)), 4096)
			for {
				var deployment appsv1.Deployment
				if err := decoder.Decode(&deployment); err != nil {
					if errors.Is(err, io.EOF) {
						t.Fatalf("rendered chart has no Deployment %q", test.deployment)
					}
					t.Fatalf("decode rendered chart: %v", err)
				}
				if deployment.Kind != "Deployment" || deployment.Name != test.deployment {
					continue
				}
				if deployment.Spec.Strategy.Type != test.strategy {
					t.Fatalf("%s strategy = %q, want %q to preserve one journal per Service", test.deployment, deployment.Spec.Strategy.Type, test.strategy)
				}
				if test.strategy == appsv1.RecreateDeploymentStrategyType && deployment.Spec.Strategy.RollingUpdate != nil {
					t.Fatal("Recreate deployment must not contain rollingUpdate options")
				}
				return
			}
		})
	}
}
