package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply pending migrations",
	Long:  "Apply all pending versioned and repeatable migrations to the target keyspace.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		dryRun, _ := cmd.Flags().GetBool("dry-run")
		target, _ := cmd.Flags().GetString("target")

		ctx, err := migration.NewExecutionContext(cfg, log)
		if err != nil {
			return err
		}
		defer ctx.Close()

		ctx.DryRun = dryRun

		// Acquire lock (skip for dry run)
		if !dryRun {
			log.Info().Msg("Acquiring migration lock...")
			if err := ctx.LockManager.Acquire(cfg.LockTimeout); err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}
			defer func() {
				if err := ctx.LockManager.Release(); err != nil {
					log.Error().Err(err).Msg("Failed to release lock")
				}
			}()
		}

		// Scan migrations directory
		scanned, err := migration.ScanMigrationsDir(cfg.MigrationsDir)
		if err != nil {
			return err
		}

		if len(scanned) == 0 {
			log.Info().Str("dir", cfg.MigrationsDir).Msg("No migration files found")
			return nil
		}

		// Get applied migrations
		applied, err := ctx.MetadataManager.GetAppliedMigrations()
		if err != nil {
			return fmt.Errorf("failed to get applied migrations: %w", err)
		}

		// Validate checksums of applied migrations
		resolver := migration.NewResolver(scanned)
		if errors := resolver.ValidateAppliedChecksums(applied); len(errors) > 0 {
			log.Error().Msg("Checksum validation failed:")
			for _, e := range errors {
				log.Error().Msg("  " + e)
			}
			return fmt.Errorf("checksum validation failed — run 'scylla-migrate validate' for details or 'scylla-migrate repair' to fix")
		}

		// Resolve pending migrations
		pending, err := resolver.GetPendingMigrations(applied)
		if err != nil {
			return err
		}

		// Filter by target version if specified
		if target != "" {
			pending = resolver.FilterUpToTarget(pending, target)
		}

		if len(pending) == 0 {
			log.Info().Msg("Schema is up to date — no pending migrations")
			return nil
		}

		// Execute
		executor := migration.NewExecutor(ctx)
		successCount, err := executor.ExecuteAll(pending)

		if err != nil {
			log.Error().
				Int("applied", successCount).
				Int("total", len(pending)).
				Err(err).
				Msg("Migration failed")
			return err
		}

		if dryRun {
			log.Info().Int("count", len(pending)).Msg("Dry run complete — no changes applied")
		} else {
			log.Info().Int("count", successCount).Msg("All migrations applied successfully")
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.Flags().Bool("dry-run", false, "show migrations without applying them")
	migrateCmd.Flags().String("target", "", "target version to migrate to (e.g., 003)")
}
