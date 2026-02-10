package migration

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/scylla-migrate/scylla-migrate/internal/config"
	"github.com/scylla-migrate/scylla-migrate/internal/driver"
	"github.com/scylla-migrate/scylla-migrate/internal/lock"
	"github.com/scylla-migrate/scylla-migrate/internal/schema"
)

type ExecutionContext struct {
	Session         *driver.Session
	Config          *config.Config
	MetadataManager *schema.MetadataManager
	LockManager     *lock.LockManager
	Logger          zerolog.Logger
	DryRun          bool
	hostname        string
}

func NewExecutionContext(cfg *config.Config, logger zerolog.Logger) (*ExecutionContext, error) {
	session, err := driver.NewSession(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	if err := schema.InitializeMetadata(session, cfg, logger); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to initialize metadata: %w", err)
	}

	metadataManager := schema.NewMetadataManager(session, cfg.MetadataKeyspace, logger)
	lockManager := lock.NewLockManager(session, cfg.MetadataKeyspace, logger)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return &ExecutionContext{
		Session:         session,
		Config:          cfg,
		MetadataManager: metadataManager,
		LockManager:     lockManager,
		Logger:          logger,
		hostname:        hostname,
	}, nil
}

func (ctx *ExecutionContext) Close() {
	ctx.Session.Close()
}

type Executor struct {
	ctx *ExecutionContext
}

func NewExecutor(ctx *ExecutionContext) *Executor {
	return &Executor{ctx: ctx}
}

func (e *Executor) Execute(mig *Migration) (retErr error) {
	start := time.Now()
	rec := toRecord(mig)

	// Panic recovery — record failure and re-panic
	if !e.ctx.DryRun {
		defer func() {
			if r := recover(); r != nil {
				_ = e.ctx.MetadataManager.RecordMigration(rec, time.Since(start), false, e.ctx.hostname)
				panic(r) // re-panic after recording failure
			}
		}()
	}

	if e.ctx.DryRun {
		e.ctx.Logger.Info().
			Str("version", mig.Version).
			Str("description", mig.Description).
			Str("type", string(mig.Type)).
			Int("statements", len(mig.Statements)).
			Msg("[DRY RUN] Would apply migration")

		for i, stmt := range mig.Statements {
			e.ctx.Logger.Info().
				Int("statement", i+1).
				Str("cql", truncateStr(stmt, 120)).
				Msg("[DRY RUN] Would execute")
		}
		return nil
	}

	if len(mig.Statements) == 0 {
		e.ctx.Logger.Warn().
			Str("version", mig.Version).
			Str("file", mig.Filename).
			Msg("Migration file contains no executable statements")
	}

	e.ctx.Logger.Info().
		Str("version", mig.Version).
		Str("description", mig.Description).
		Int("statements", len(mig.Statements)).
		Msg("Applying migration")

	for i, stmt := range mig.Statements {
		e.ctx.Logger.Debug().
			Int("statement", i+1).
			Int("total", len(mig.Statements)).
			Msg("Executing statement")

		if err := e.ctx.Session.Execute(stmt); err != nil {
			_ = e.ctx.MetadataManager.RecordMigration(rec, time.Since(start), false, e.ctx.hostname)
			return fmt.Errorf("failed to execute statement %d in %s: %w", i+1, mig.Filename, err)
		}

		if IsDDL(stmt) {
			e.ctx.Logger.Debug().Msg("Waiting for schema agreement after DDL")
			if err := e.ctx.Session.WaitForSchemaAgreement(e.ctx.Config.SchemaAgreementTimeout); err != nil {
				_ = e.ctx.MetadataManager.RecordMigration(rec, time.Since(start), false, e.ctx.hostname)
				return fmt.Errorf("schema agreement timeout after statement %d in %s: %w", i+1, mig.Filename, err)
			}
		}
	}

	executionTime := time.Since(start)
	if err := e.ctx.MetadataManager.RecordMigration(rec, executionTime, true, e.ctx.hostname); err != nil {
		return fmt.Errorf("migration executed successfully but failed to record metadata: %w", err)
	}

	e.ctx.Logger.Info().
		Str("version", mig.Version).
		Str("description", mig.Description).
		Dur("duration", executionTime).
		Msg("Migration applied successfully")

	return nil
}

func (e *Executor) ExecuteAll(migrations []*Migration) (int, error) {
	total := len(migrations)
	for i, mig := range migrations {
		e.ctx.Logger.Info().
			Int("current", i+1).
			Int("total", total).
			Str("version", mig.Version).
			Msg("Processing migration")

		if err := e.Execute(mig); err != nil {
			return i, err
		}
	}
	return total, nil
}

func toRecord(mig *Migration) schema.MigrationRecord {
	version := mig.Version
	if mig.Type == TypeRepeatable {
		version = mig.Version + "_" + mig.Description
	}
	return schema.MigrationRecord{
		Version:     version,
		Description: mig.Description,
		Type:        string(mig.Type),
		Filename:    mig.Filename,
		Checksum:    mig.Checksum,
	}
}

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
