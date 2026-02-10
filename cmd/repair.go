package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Repair migration metadata",
	Long:  "Fix migration metadata: recalculate checksums for applied migrations or remove failed migration records.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		recalcChecksums, _ := cmd.Flags().GetBool("recalculate-checksums")
		removeFailed, _ := cmd.Flags().GetBool("remove-failed")

		if !recalcChecksums && !removeFailed {
			return fmt.Errorf("specify at least one repair action: --recalculate-checksums or --remove-failed")
		}

		ctx, err := migration.NewExecutionContext(cfg, log)
		if err != nil {
			return err
		}
		defer ctx.Close()

		if recalcChecksums {
			log.Info().Msg("Recalculating checksums for applied migrations...")

			scanned, err := migration.ScanMigrationsDir(cfg.MigrationsDir)
			if err != nil {
				return err
			}

			fileMap := make(map[string]*migration.Migration)
			for _, mig := range scanned {
				if mig.Type == migration.TypeVersioned {
					if err := migration.ParseMigrationFile(mig); err != nil {
						log.Warn().Str("file", mig.Filename).Err(err).Msg("Failed to parse, skipping")
						continue
					}
					fileMap[mig.Version] = mig
				}
			}

			applied, err := ctx.MetadataManager.GetAppliedMigrations()
			if err != nil {
				return fmt.Errorf("failed to get applied migrations: %w", err)
			}

			updated := 0
			for _, a := range applied {
				if !a.Success || a.Type != "versioned" {
					continue
				}
				fileMig, exists := fileMap[a.Version]
				if !exists {
					log.Warn().Str("version", a.Version).Msg("No file found for applied migration, skipping")
					continue
				}
				if fileMig.Checksum != a.Checksum {
					if err := ctx.MetadataManager.UpdateChecksum(a.Version, fileMig.Checksum); err != nil {
						log.Error().Str("version", a.Version).Err(err).Msg("Failed to update checksum")
						continue
					}
					log.Info().
						Str("version", a.Version).
						Str("old", a.Checksum).
						Str("new", fileMig.Checksum).
						Msg("Updated checksum")
					updated++
				}
			}

			log.Info().Int("updated", updated).Msg("Checksum recalculation complete")
		}

		if removeFailed {
			log.Info().Msg("Removing failed migration records...")

			failed, err := ctx.MetadataManager.GetFailedMigrations()
			if err != nil {
				return fmt.Errorf("failed to get failed migrations: %w", err)
			}

			removed := 0
			for _, f := range failed {
				if err := ctx.MetadataManager.RemoveMigration(f.Version); err != nil {
					log.Error().Str("version", f.Version).Err(err).Msg("Failed to remove record")
					continue
				}
				log.Info().Str("version", f.Version).Str("description", f.Description).Msg("Removed failed migration record")
				removed++
			}

			log.Info().Int("removed", removed).Msg("Failed migration cleanup complete")
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(repairCmd)
	repairCmd.Flags().Bool("recalculate-checksums", false, "recalculate checksums for all applied migrations")
	repairCmd.Flags().Bool("remove-failed", false, "remove failed migration records from metadata")
}
