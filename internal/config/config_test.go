package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validTestConfig() *Config {
	return &Config{
		Hosts:                  []string{"localhost:9042"},
		Keyspace:               "test_ks",
		MigrationsDir:          "./migrations",
		Consistency:            "quorum",
		Timeout:                30_000_000_000,
		LockTimeout:            60_000_000_000,
		MetadataKeyspace:       "scylla_migrate",
		SchemaAgreementTimeout: 30_000_000_000,
		ProtocolVersion:        4,
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := validTestConfig()
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestConfig_Validate_MissingHosts(t *testing.T) {
	cfg := validTestConfig()
	cfg.Hosts = nil
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "host")
}

func TestConfig_Validate_MissingKeyspace(t *testing.T) {
	cfg := validTestConfig()
	cfg.Keyspace = ""
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "keyspace")
}

func TestConfig_Validate_InvalidConsistency(t *testing.T) {
	cfg := validTestConfig()
	cfg.Consistency = "invalid_level"
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consistency")
}

func TestConfig_Validate_InvalidProtocolVersion(t *testing.T) {
	cfg := validTestConfig()
	cfg.ProtocolVersion = 0
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "protocol_version")

	cfg.ProtocolVersion = 6
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "protocol_version")
}

func TestConfig_Validate_InvalidKeyspaceName(t *testing.T) {
	cfg := validTestConfig()
	cfg.Keyspace = "invalid-keyspace"
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")
}

func TestConfig_Validate_SSLClientCertKeyPairing(t *testing.T) {
	cfg := validTestConfig()
	cfg.SSL.Enabled = true
	cfg.SSL.CACert = "/path/to/ca.crt"
	cfg.SSL.ClientCert = "/path/to/client.crt"
	cfg.SSL.ClientKey = "" // missing key
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ssl.client_cert")
}

func TestConfig_GetConsistency(t *testing.T) {
	tests := []struct {
		level   string
		wantErr bool
	}{
		{"one", false},
		{"quorum", false},
		{"all", false},
		{"local_quorum", false},
		{"each_quorum", false},
		{"local_one", false},
		{"any", false},
		{"two", false},
		{"three", false},
		{"invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			cfg := &Config{Consistency: tt.level}
			_, err := cfg.GetConsistency()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_ReplicationCQL_SimpleStrategy(t *testing.T) {
	cfg := &Config{
		MetadataReplication: ReplicationConfig{
			Class:             "SimpleStrategy",
			ReplicationFactor: 3,
		},
	}
	cql := cfg.ReplicationCQL()
	assert.Contains(t, cql, "SimpleStrategy")
	assert.Contains(t, cql, "3")
}

func TestConfig_ReplicationCQL_NetworkTopologyStrategy(t *testing.T) {
	cfg := &Config{
		MetadataReplication: ReplicationConfig{
			Class: "NetworkTopologyStrategy",
			Datacenters: map[string]int{
				"dc1": 3,
			},
		},
	}
	cql := cfg.ReplicationCQL()
	assert.Contains(t, cql, "NetworkTopologyStrategy")
	assert.Contains(t, cql, "dc1")
	assert.Contains(t, cql, "3")
}
