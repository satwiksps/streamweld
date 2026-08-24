package streamweld_test

import (
	"os"
	"strings"
	"testing"
)

func TestRelayAdvertisesPodIPWithExplicitTLSIdentity(t *testing.T) {
	t.Parallel()

	deploymentBytes, err := os.ReadFile("templates/proxy-deployment.yaml")
	if err != nil {
		t.Fatalf("read proxy Deployment template: %v", err)
	}
	deployment := string(deploymentBytes)
	for _, required := range []string{
		"fieldPath: status.podIP",
		"STREAMWELD_RELAY_ADVERTISE_HOST",
		`value: "$(POD_IP)"`,
		"STREAMWELD_RELAY_TLS_SERVER_NAME",
		`%s.$(POD_NAMESPACE).svc`,
	} {
		if !strings.Contains(deployment, required) {
			t.Errorf("proxy Deployment is missing relay setting %q", required)
		}
	}
	for _, invalid := range []string{
		"STREAMWELD_REPLICA_ID",
		"subdomain:",
		"STREAMWELD_RELAY_ADVERTISE_URL",
		"https://$(POD_NAME).",
	} {
		if strings.Contains(deployment, invalid) {
			t.Errorf("proxy Deployment retains unsafe relay addressing %q", invalid)
		}
	}
}
