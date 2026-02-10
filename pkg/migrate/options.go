package migrate

import (
	"time"

	"github.com/scylla-migrate/scylla-migrate/internal/config"
)

type Option func(*config.Config)

func WithHosts(hosts ...string) Option {
	return func(c *config.Config) {
		c.Hosts = hosts
	}
}

func WithKeyspace(keyspace string) Option {
	return func(c *config.Config) {
		c.Keyspace = keyspace
	}
}

func WithMigrationsDir(dir string) Option {
	return func(c *config.Config) {
		c.MigrationsDir = dir
	}
}

func WithAuth(username, password string) Option {
	return func(c *config.Config) {
		c.Username = username
		c.Password = password
	}
}

func WithConsistency(level string) Option {
	return func(c *config.Config) {
		c.Consistency = level
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *config.Config) {
		c.Timeout = timeout
	}
}

func WithMetadataKeyspace(keyspace string) Option {
	return func(c *config.Config) {
		c.MetadataKeyspace = keyspace
	}
}

func WithSSL(caCert, clientCert, clientKey string) Option {
	return func(c *config.Config) {
		c.SSL.Enabled = true
		c.SSL.CACert = caCert
		c.SSL.ClientCert = clientCert
		c.SSL.ClientKey = clientKey
	}
}
