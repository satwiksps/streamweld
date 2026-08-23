package streamweld_test

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestRedisPasswordDefaultsToGeneratedSecret(t *testing.T) {
	valuesBytes, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	var values struct {
		Redis struct {
			Auth map[string]any `yaml:"auth"`
		} `yaml:"redis"`
	}
	if err := yaml.Unmarshal(valuesBytes, &values); err != nil {
		t.Fatalf("decode chart values: %v", err)
	}
	if password, configured := values.Redis.Auth["password"]; configured {
		t.Fatalf("default Redis password must be omitted so Helm generates it, got %#v", password)
	}

	helperBytes, err := os.ReadFile("templates/_helpers.tpl")
	if err != nil {
		t.Fatalf("read chart helpers: %v", err)
	}
	helper := string(helperBytes)
	for _, required := range []string{
		`define "streamweld.redisPassword"`,
		`lookup "v1" "Secret"`,
		`randAlphaNum 40`,
	} {
		if !strings.Contains(helper, required) {
			t.Errorf("Redis password helper is missing %q", required)
		}
	}
}

func TestRedisPasswordOverrideMustBeNonEmpty(t *testing.T) {
	schemaBytes, err := os.ReadFile("values.schema.json")
	if err != nil {
		t.Fatalf("read chart values schema: %v", err)
	}
	var schema struct {
		Properties struct {
			Redis struct {
				Properties struct {
					Auth struct {
						Required   []string `json:"required"`
						Properties struct {
							Password struct {
								Type    string `json:"type"`
								Pattern string `json:"pattern"`
							} `json:"password"`
						} `json:"properties"`
					} `json:"auth"`
				} `json:"properties"`
			} `json:"redis"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		t.Fatalf("decode chart values schema: %v", err)
	}
	auth := schema.Properties.Redis.Properties.Auth
	if slices.Contains(auth.Required, "password") {
		t.Error("redis.auth.password must remain optional so the chart can generate it")
	}
	if auth.Properties.Password.Type != "string" || !strings.Contains(auth.Properties.Password.Pattern, "+") {
		t.Fatalf("redis.auth.password must accept only non-empty strings, got type %q pattern %q",
			auth.Properties.Password.Type, auth.Properties.Password.Pattern)
	}
}
