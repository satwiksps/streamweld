package proxy

import (
	"strings"
	"testing"
	"time"
)

func TestRelayConfigRequiresMTLSOrExplicitLoopbackDevelopmentMode(t *testing.T) {
	t.Parallel()
	base := DefaultConfig()
	base.BackendURL = "http://backend.example.test"
	base.ListenAddress = "127.0.0.1:0"
	base.JournalBackend = JournalBackendRedis
	base.RedisURL = "redis://redis.example.test:6379/0"
	base.ReplicaID = "replica-a"
	base.RelayAdvertiseURL = "http://127.0.0.1:18081"
	base.RelayListenAddress = "127.0.0.1:18081"
	base.RelayInsecureDevMode = true
	if err := base.Validate(); err != nil {
		t.Fatalf("loopback insecure development relay rejected: %v", err)
	}

	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"public listen", func(config *Config) { config.RelayListenAddress = "0.0.0.0:18081" }, "loopback"},
		{"public advertise", func(config *Config) { config.RelayAdvertiseURL = "http://relay.example.test:18081" }, "loopback"},
		{"https insecure", func(config *Config) { config.RelayAdvertiseURL = "https://127.0.0.1:18081" }, "must use http"},
		{"memory journal", func(config *Config) { config.JournalBackend = JournalBackendMemory }, "requires the Redis"},
		{"invalid replica", func(config *Config) { config.ReplicaID = "replica a" }, "replica ID"},
		{"short presence", func(config *Config) { config.RelayPresenceTTL = 2 * config.RelayHeartbeatInterval }, "must exceed twice"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.edit(&config)
			if err := config.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Config.Validate() error = %v, want %q", err, test.want)
			}
		})
	}

	production := base
	production.RelayInsecureDevMode = false
	production.RelayAdvertiseURL = "https://relay-a.internal:8443"
	if err := production.Validate(); err == nil || !strings.Contains(err.Error(), "production relay requires") {
		t.Fatalf("production config without mTLS material error = %v", err)
	}
	production.RelayCAFile = "ca.pem"
	production.RelayCertificateFile = "tls.crt"
	production.RelayPrivateKeyFile = "tls.key"
	if err := production.Validate(); err != nil {
		t.Fatalf("production mTLS config rejected before file loading: %v", err)
	}
}

func TestConfigFromEnvLoadsRelaySettings(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"STREAMWELD_REPLICA_ID":               "replica-env",
		"STREAMWELD_RELAY_LISTEN":             "127.0.0.1:19001",
		"STREAMWELD_RELAY_ADVERTISE_URL":      "http://127.0.0.1:19001",
		"STREAMWELD_RELAY_CA_FILE":            "relay-ca.pem",
		"STREAMWELD_RELAY_CERT_FILE":          "relay.crt",
		"STREAMWELD_RELAY_KEY_FILE":           "relay.key",
		"STREAMWELD_RELAY_INSECURE_DEV_MODE":  "true",
		"STREAMWELD_RELAY_HEARTBEAT_INTERVAL": "750ms",
		"STREAMWELD_RELAY_PRESENCE_TTL":       "3s",
	}
	config, err := ConfigFromEnv(mapLookup(values))
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if config.ReplicaID != "replica-env" || config.RelayListenAddress != "127.0.0.1:19001" ||
		config.RelayAdvertiseURL != "http://127.0.0.1:19001" || !config.RelayInsecureDevMode ||
		config.RelayHeartbeatInterval != 750*time.Millisecond || config.RelayPresenceTTL != 3*time.Second {
		t.Fatalf("relay environment settings not applied: %+v", config)
	}
	if config.RelayCAFile != "relay-ca.pem" || config.RelayCertificateFile != "relay.crt" || config.RelayPrivateKeyFile != "relay.key" {
		t.Fatalf("relay mTLS paths not applied: %+v", config)
	}
}
