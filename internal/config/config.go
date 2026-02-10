package config

import (
	"fmt"
	"regexp"
	"time"

	"github.com/gocql/gocql"
	"github.com/spf13/viper"
)

var validIdentifier = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

type Config struct {
	Hosts                  []string          `mapstructure:"hosts" yaml:"hosts"`
	Keyspace               string            `mapstructure:"keyspace" yaml:"keyspace"`
	MigrationsDir          string            `mapstructure:"migrations_dir" yaml:"migrations_dir"`
	Username               string            `mapstructure:"username" yaml:"username"`
	Password               string            `mapstructure:"password" yaml:"password"`
	SSL                    SSLConfig         `mapstructure:"ssl" yaml:"ssl"`
	Consistency            string            `mapstructure:"consistency" yaml:"consistency"`
	Timeout                time.Duration     `mapstructure:"timeout" yaml:"timeout"`
	ConnectionTimeout      time.Duration     `mapstructure:"connection_timeout" yaml:"connection_timeout"`
	LockTimeout            time.Duration     `mapstructure:"lock_timeout" yaml:"lock_timeout"`
	SchemaAgreementTimeout time.Duration     `mapstructure:"schema_agreement_timeout" yaml:"schema_agreement_timeout"`
	MetadataKeyspace       string            `mapstructure:"metadata_keyspace" yaml:"metadata_keyspace"`
	MetadataReplication    ReplicationConfig `mapstructure:"metadata_replication" yaml:"metadata_replication"`
	MaxRetries             int               `mapstructure:"max_retries" yaml:"max_retries"`
	ProtocolVersion        int               `mapstructure:"protocol_version" yaml:"protocol_version"`
}

type SSLConfig struct {
	Enabled    bool   `mapstructure:"enabled" yaml:"enabled"`
	CACert     string `mapstructure:"ca_cert" yaml:"ca_cert"`
	ClientCert string `mapstructure:"client_cert" yaml:"client_cert"`
	ClientKey  string `mapstructure:"client_key" yaml:"client_key"`
	SkipVerify bool   `mapstructure:"skip_verify" yaml:"skip_verify"`
}

type ReplicationConfig struct {
	Class             string         `mapstructure:"class" yaml:"class"`
	ReplicationFactor int            `mapstructure:"replication_factor" yaml:"replication_factor"`
	Datacenters       map[string]int `mapstructure:"datacenters" yaml:"datacenters"`
}

func Load() (*Config, error) {
	cfg := &Config{
		Hosts:                  []string{"localhost:9042"},
		MigrationsDir:          "./migrations",
		Consistency:            "quorum",
		Timeout:                30 * time.Second,
		ConnectionTimeout:      10 * time.Second,
		LockTimeout:            60 * time.Second,
		SchemaAgreementTimeout: 30 * time.Second,
		MetadataKeyspace:       "scylla_migrate",
		MetadataReplication: ReplicationConfig{
			Class:             "SimpleStrategy",
			ReplicationFactor: 1,
		},
		MaxRetries:      3,
		ProtocolVersion: 4,
	}

	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Override with CLI flags if set
	if hosts := viper.GetStringSlice("hosts"); len(hosts) > 0 {
		cfg.Hosts = hosts
	}
	if ks := viper.GetString("keyspace"); ks != "" {
		cfg.Keyspace = ks
	}
	if dir := viper.GetString("migrations_dir"); dir != "" {
		cfg.MigrationsDir = dir
	}
	if u := viper.GetString("username"); u != "" {
		cfg.Username = u
	}
	if p := viper.GetString("password"); p != "" {
		cfg.Password = p
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return fmt.Errorf("at least one host must be specified")
	}

	if c.Keyspace == "" {
		return fmt.Errorf("keyspace must be specified")
	}
	if !validIdentifier.MatchString(c.Keyspace) {
		return fmt.Errorf("keyspace name %q contains invalid characters (must be alphanumeric/underscore, starting with a letter)", c.Keyspace)
	}

	if c.MigrationsDir == "" {
		return fmt.Errorf("migrations_dir must be specified")
	}

	if c.MetadataKeyspace == "" {
		return fmt.Errorf("metadata_keyspace must be specified")
	}
	if !validIdentifier.MatchString(c.MetadataKeyspace) {
		return fmt.Errorf("metadata_keyspace name %q contains invalid characters", c.MetadataKeyspace)
	}

	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	if c.LockTimeout <= 0 {
		return fmt.Errorf("lock_timeout must be positive")
	}

	if c.SchemaAgreementTimeout <= 0 {
		return fmt.Errorf("schema_agreement_timeout must be positive")
	}

	if c.ProtocolVersion < 1 || c.ProtocolVersion > 5 {
		return fmt.Errorf("protocol_version must be between 1 and 5")
	}

	if _, err := c.GetConsistency(); err != nil {
		return err
	}

	if c.SSL.Enabled {
		if c.SSL.CACert == "" {
			return fmt.Errorf("ssl.ca_cert must be specified when SSL is enabled")
		}
		// Client cert and key must both be present or both absent
		if (c.SSL.ClientCert != "") != (c.SSL.ClientKey != "") {
			return fmt.Errorf("ssl.client_cert and ssl.client_key must both be specified or both omitted")
		}
	}

	return nil
}

func (c *Config) GetConsistency() (gocql.Consistency, error) {
	switch c.Consistency {
	case "any":
		return gocql.Any, nil
	case "one":
		return gocql.One, nil
	case "two":
		return gocql.Two, nil
	case "three":
		return gocql.Three, nil
	case "quorum":
		return gocql.Quorum, nil
	case "all":
		return gocql.All, nil
	case "local_quorum":
		return gocql.LocalQuorum, nil
	case "each_quorum":
		return gocql.EachQuorum, nil
	case "local_one":
		return gocql.LocalOne, nil
	default:
		return 0, fmt.Errorf("unsupported consistency level: %s", c.Consistency)
	}
}

func (c *Config) ReplicationCQL() string {
	if c.MetadataReplication.Class == "NetworkTopologyStrategy" && len(c.MetadataReplication.Datacenters) > 0 {
		cql := "{'class': 'NetworkTopologyStrategy'"
		for dc, rf := range c.MetadataReplication.Datacenters {
			cql += fmt.Sprintf(", '%s': %d", dc, rf)
		}
		cql += "}"
		return cql
	}

	rf := c.MetadataReplication.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	return fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': %d}", rf)
}
