package schema

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/scylla-migrate/scylla-migrate/internal/config"
	"github.com/scylla-migrate/scylla-migrate/internal/driver"
)

func InitializeMetadata(session *driver.Session, cfg *config.Config, logger zerolog.Logger) error {
	keyspace := cfg.MetadataKeyspace
	replication := cfg.ReplicationCQL()

	logger.Debug().
		Str("keyspace", keyspace).
		Str("replication", replication).
		Msg("Initializing metadata keyspace")

	// Create metadata keyspace
	createKS := fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s AND durable_writes = true`,
		keyspace, replication,
	)
	if err := session.Execute(createKS); err != nil {
		return fmt.Errorf("failed to create metadata keyspace: %w", err)
	}

	if err := session.WaitForSchemaAgreement(cfg.SchemaAgreementTimeout); err != nil {
		return fmt.Errorf("schema agreement timeout after creating keyspace: %w", err)
	}

	// Create schema_migrations table
	createMigrations := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.schema_migrations (
			version TEXT,
			description TEXT,
			type TEXT,
			script TEXT,
			checksum TEXT,
			applied_by TEXT,
			applied_at TIMESTAMP,
			execution_time_ms INT,
			success BOOLEAN,
			PRIMARY KEY (version)
		) WITH comment = 'scylla-migrate: tracks applied schema migrations'`,
		keyspace,
	)
	if err := session.Execute(createMigrations); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	if err := session.WaitForSchemaAgreement(cfg.SchemaAgreementTimeout); err != nil {
		return fmt.Errorf("schema agreement timeout after creating migrations table: %w", err)
	}

	// Create schema_lock table
	createLock := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.schema_lock (
			lock_id TEXT PRIMARY KEY,
			locked_by TEXT,
			locked_at TIMESTAMP,
			expires_at TIMESTAMP
		) WITH comment = 'scylla-migrate: distributed lock for migration execution'
		  AND default_time_to_live = 3600`,
		keyspace,
	)
	if err := session.Execute(createLock); err != nil {
		return fmt.Errorf("failed to create schema_lock table: %w", err)
	}

	if err := session.WaitForSchemaAgreement(cfg.SchemaAgreementTimeout); err != nil {
		return fmt.Errorf("schema agreement timeout after creating lock table: %w", err)
	}

	logger.Info().Str("keyspace", keyspace).Msg("Metadata tables initialized")
	return nil
}
