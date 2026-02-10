package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"

	"github.com/scylla-migrate/scylla-migrate/internal/config"
)

type ClusterMetadata struct {
	ClusterName string
	Hosts       []string
	Keyspaces   []string
	SchemaVer   string
}

type Session struct {
	session *gocql.Session
	config  *config.Config
	Logger  zerolog.Logger
}

func NewSession(cfg *config.Config, logger zerolog.Logger) (*Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Consistency = mustConsistency(cfg.Consistency)
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectionTimeout
	cluster.ProtoVersion = cfg.ProtocolVersion
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: cfg.MaxRetries,
		Min:        500 * time.Millisecond,
		Max:        5 * time.Second,
	}

	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	if cfg.SSL.Enabled {
		tlsConfig, err := buildTLSConfig(cfg.SSL)
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		cluster.SslOpts = &gocql.SslOptions{
			Config: tlsConfig,
		}
	}

	logger.Debug().
		Strs("hosts", cfg.Hosts).
		Str("consistency", cfg.Consistency).
		Msg("Connecting to cluster")

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cluster: %w", err)
	}

	logger.Info().Msg("Connected to cluster")

	return &Session{
		session: session,
		config:  cfg,
		Logger:  logger,
	}, nil
}

func (s *Session) Close() {
	if s.session != nil && !s.session.Closed() {
		s.session.Close()
		s.Logger.Debug().Msg("Session closed")
	}
}

func (s *Session) Execute(query string, args ...interface{}) error {
	s.Logger.Debug().Str("query", truncate(query, 200)).Msg("Executing query")
	return s.session.Query(query, args...).Exec()
}

func (s *Session) Query(query string, args ...interface{}) *gocql.Query {
	return s.session.Query(query, args...)
}

func (s *Session) WaitForSchemaAgreement(timeout time.Duration) error {
	s.Logger.Debug().Dur("timeout", timeout).Msg("Waiting for schema agreement")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("schema agreement not reached within %s: %w", timeout, err)
	}

	s.Logger.Debug().Msg("Schema agreement reached")
	return nil
}

func (s *Session) GetClusterMetadata() (*ClusterMetadata, error) {
	meta := &ClusterMetadata{
		Hosts: s.config.Hosts,
	}

	// Get cluster name
	var clusterName string
	if err := s.session.Query("SELECT cluster_name FROM system.local WHERE key='local'").Scan(&clusterName); err != nil {
		s.Logger.Warn().Err(err).Msg("Failed to get cluster name")
		meta.ClusterName = "unknown"
	} else {
		meta.ClusterName = clusterName
	}

	// Get schema version
	var schemaVer string
	if err := s.session.Query("SELECT schema_version FROM system.local WHERE key='local'").Scan(&schemaVer); err != nil {
		meta.SchemaVer = "unknown"
	} else {
		meta.SchemaVer = schemaVer
	}

	// Get keyspaces
	iter := s.session.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()
	var ks string
	for iter.Scan(&ks) {
		meta.Keyspaces = append(meta.Keyspaces, ks)
	}
	if err := iter.Close(); err != nil {
		s.Logger.Warn().Err(err).Msg("Failed to list keyspaces")
	}

	return meta, nil
}

func (s *Session) KeyspaceExists(keyspace string) (bool, error) {
	var count int
	err := s.session.Query(
		"SELECT COUNT(*) FROM system_schema.keyspaces WHERE keyspace_name = ?",
		keyspace,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func buildTLSConfig(ssl config.SSLConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: ssl.SkipVerify,
	}

	if ssl.CACert != "" {
		caCert, err := os.ReadFile(ssl.CACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if ssl.ClientCert != "" && ssl.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(ssl.ClientCert, ssl.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func mustConsistency(level string) gocql.Consistency {
	switch level {
	case "any":
		return gocql.Any
	case "one":
		return gocql.One
	case "two":
		return gocql.Two
	case "three":
		return gocql.Three
	case "all":
		return gocql.All
	case "local_quorum":
		return gocql.LocalQuorum
	case "each_quorum":
		return gocql.EachQuorum
	case "local_one":
		return gocql.LocalOne
	default:
		return gocql.Quorum
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
