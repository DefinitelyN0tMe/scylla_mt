package cmd

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
	"github.com/scylla-migrate/scylla-migrate/internal/schema"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "Rollback migrations using undo scripts",
	Long:  "Rollback applied migrations by executing their corresponding undo migration files (U prefix).",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		target, _ := cmd.Flags().GetString("to")
		steps, _ := cmd.Flags().GetInt("steps")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

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

		// Get applied migrations (versioned only, sorted by version desc)
		applied, err := ctx.MetadataManager.GetAppliedMigrations()
		if err != nil {
			return fmt.Errorf("failed to get applied migrations: %w", err)
		}

		var versioned []schema.AppliedMigration
		for _, a := range applied {
			if a.Success && a.Type == "versioned" {
				versioned = append(versioned, a)
			}
		}
		sort.Slice(versioned, func(i, j int) bool {
			return migration.CompareVersions(versioned[i].Version, versioned[j].Version) > 0 // descending
		})

		// Determine which migrations to rollback
		var toRollback []schema.AppliedMigration
		if target != "" {
			for _, a := range versioned {
				if migration.CompareVersions(a.Version, target) > 0 {
					toRollback = append(toRollback, a)
				}
			}
		} else {
			if steps <= 0 {
				steps = 1
			}
			if steps > len(versioned) {
				steps = len(versioned)
			}
			toRollback = versioned[:steps]
		}

		if len(toRollback) == 0 {
			log.Info().Msg("No migrations to rollback")
			return nil
		}

		// Scan migration files to find undo scripts
		scanned, err := migration.ScanMigrationsDir(cfg.MigrationsDir)
		if err != nil {
			return err
		}
		resolver := migration.NewResolver(scanned)

		// Verify undo files exist for all target migrations
		var undoMigrations []*migration.Migration
		for _, a := range toRollback {
			undo := resolver.GetUndoMigration(a.Version)
			if undo == nil {
				return fmt.Errorf("no undo migration file found for version %s (%s) — expected U%s__*.cql",
					a.Version, a.Description, a.Version)
			}
			if err := migration.ParseMigrationFile(undo); err != nil {
				return fmt.Errorf("failed to parse undo migration %s: %w", undo.Filename, err)
			}
			undoMigrations = append(undoMigrations, undo)
		}

		// Confirm
		if !dryRun {
			fmt.Printf("\nAbout to rollback %d migration(s):\n", len(toRollback))
			for _, a := range toRollback {
				fmt.Printf("  V%s: %s\n", a.Version, a.Description)
			}
			fmt.Print("\nContinue? [y/N]: ")

			reader := bufio.NewReader(os.Stdin)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))
			if response != "y" && response != "yes" {
				log.Info().Msg("Rollback cancelled")
				return nil
			}
		}

		// Execute undo migrations — use executor for dry-run display,
		// but for real execution, run statements directly and remove the
		// versioned migration record (don't record undo as a new migration)
		if dryRun {
			executor := migration.NewExecutor(ctx)
			for _, undo := range undoMigrations {
				if err := executor.Execute(undo); err != nil {
					return err
				}
			}
			log.Info().Int("count", len(toRollback)).Msg("Dry run complete — no changes applied")
			return nil
		}

		for i, undo := range undoMigrations {
			log.Info().
				Str("version", undo.Version).
				Str("description", undo.Description).
				Msg("Rolling back migration")

			// Execute undo statements directly (don't record in metadata)
			for j, stmt := range undo.Statements {
				if err := ctx.Session.Execute(stmt); err != nil {
					return fmt.Errorf("rollback failed at version %s, statement %d: %w", undo.Version, j+1, err)
				}
				if migration.IsDDL(stmt) {
					if err := ctx.Session.WaitForSchemaAgreement(cfg.SchemaAgreementTimeout); err != nil {
						log.Warn().Err(err).Msg("Schema agreement timeout during rollback")
					}
				}
			}

			// Remove the versioned migration record from metadata
			if err := ctx.MetadataManager.RemoveMigration(toRollback[i].Version); err != nil {
				return fmt.Errorf("failed to remove migration record for version %s: %w", toRollback[i].Version, err)
			}

			log.Info().Str("version", undo.Version).Msg("Rollback applied")
		}

		log.Info().Int("count", len(toRollback)).Msg("Rollback completed successfully")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(rollbackCmd)
	rollbackCmd.Flags().String("to", "", "target version to rollback to (exclusive)")
	rollbackCmd.Flags().Int("steps", 1, "number of migrations to rollback")
	rollbackCmd.Flags().Bool("dry-run", false, "show rollback plan without executing")
}
