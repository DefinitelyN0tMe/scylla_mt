// Package migrate provides an embeddable ScyllaDB migration runner
// for use as a Go library in your applications.
//
// Example usage:
//
//	m, err := migrate.New(
//	    migrate.WithHosts("localhost:9042"),
//	    migrate.WithKeyspace("my_app"),
//	    migrate.WithMigrationsDir("./migrations"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer m.Close()
//
//	if err := m.Migrate(); err != nil {
//	    log.Fatal(err)
//	}
package migrate

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/scylla-migrate/scylla-migrate/internal/config"
	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

type Migrator struct {
	ctx    *migration.ExecutionContext
	config *config.Config
	logger zerolog.Logger
}

func New(opts ...Option) (*Migrator, error) {
	cfg := &config.Config{
		Hosts:                  []string{"localhost:9042"},
		MigrationsDir:          "./migrations",
		Consistency:            "quorum",
		Timeout:                30 * time.Second,
		ConnectionTimeout:      10 * time.Second,
		LockTimeout:            60 * time.Second,
		SchemaAgreementTimeout: 30 * time.Second,
		MetadataKeyspace:       "scylla_migrate",
		MetadataReplication: config.ReplicationConfig{
			Class:             "SimpleStrategy",
			ReplicationFactor: 1,
		},
		MaxRetries:      3,
		ProtocolVersion: 4,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger := zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05",
	}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	ctx, err := migration.NewExecutionContext(cfg, logger)
	if err != nil {
		return nil, err
	}

	return &Migrator{
		ctx:    ctx,
		config: cfg,
		logger: logger,
	}, nil
}

func (m *Migrator) Migrate() error {
	if err := m.ctx.LockManager.Acquire(m.config.LockTimeout); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer func() {
		if err := m.ctx.LockManager.Release(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to release migration lock")
		}
	}()

	scanned, err := migration.ScanMigrationsDir(m.config.MigrationsDir)
	if err != nil {
		return err
	}

	applied, err := m.ctx.MetadataManager.GetAppliedMigrations()
	if err != nil {
		return err
	}

	resolver := migration.NewResolver(scanned)
	if errors := resolver.ValidateAppliedChecksums(applied); len(errors) > 0 {
		return fmt.Errorf("checksum validation failed: %v", errors)
	}

	pending, err := resolver.GetPendingMigrations(applied)
	if err != nil {
		return err
	}

	if len(pending) == 0 {
		m.logger.Info().Msg("Schema is up to date")
		return nil
	}

	executor := migration.NewExecutor(m.ctx)
	_, err = executor.ExecuteAll(pending)
	return err
}

func (m *Migrator) Status() (int, int, error) {
	scanned, err := migration.ScanMigrationsDir(m.config.MigrationsDir)
	if err != nil {
		return 0, 0, err
	}

	applied, err := m.ctx.MetadataManager.GetAppliedMigrations()
	if err != nil {
		return 0, 0, err
	}

	resolver := migration.NewResolver(scanned)
	pending, err := resolver.GetPendingMigrations(applied)
	if err != nil {
		return 0, 0, err
	}

	return len(applied), len(pending), nil
}

func (m *Migrator) Close() error {
	m.ctx.Close()
	return nil
}
