package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate migration checksums",
	Long:  "Verify that applied migration files have not been modified since they were applied.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		ctx, err := migration.NewExecutionContext(cfg, log)
		if err != nil {
			return err
		}
		defer ctx.Close()

		scanned, err := migration.ScanMigrationsDir(cfg.MigrationsDir)
		if err != nil {
			return err
		}

		applied, err := ctx.MetadataManager.GetAppliedMigrations()
		if err != nil {
			return fmt.Errorf("failed to get applied migrations: %w", err)
		}

		resolver := migration.NewResolver(scanned)
		errors := resolver.ValidateAppliedChecksums(applied)

		if len(errors) > 0 {
			log.Error().Msg("Validation failed:")
			for _, e := range errors {
				log.Error().Msg("  " + e)
			}
			return fmt.Errorf("found %d validation error(s) — run 'scylla-migrate repair --recalculate-checksums' to fix", len(errors))
		}

		log.Info().Int("checked", len(applied)).Msg("All migration checksums are valid")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
}
