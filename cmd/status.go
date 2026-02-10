package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  "Display a table of all migrations with their current status (applied or pending).",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		format, _ := cmd.Flags().GetString("format")

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

		appliedMap := make(map[string]struct {
			AppliedAt string
			Checksum  string
			Success   bool
		})
		for _, a := range applied {
			appliedMap[a.Version] = struct {
				AppliedAt string
				Checksum  string
				Success   bool
			}{
				AppliedAt: a.AppliedAt.Format("2006-01-02 15:04:05"),
				Checksum:  a.Checksum,
				Success:   a.Success,
			}
		}

		// Parse all migration files to get checksums
		for _, mig := range scanned {
			_ = migration.ParseMigrationFile(mig)
		}

		type statusEntry struct {
			Version       string `json:"version"`
			Description   string `json:"description"`
			Type          string `json:"type"`
			Status        string `json:"status"`
			AppliedAt     string `json:"applied_at"`
			ChecksumMatch string `json:"checksum_match"`
		}

		var entries []statusEntry
		appliedCount := 0
		pendingCount := 0

		for _, mig := range scanned {
			entry := statusEntry{
				Version:     mig.Version,
				Description: mig.Description,
				Type:        string(mig.Type),
			}

			key := mig.Version
			if mig.Type == migration.TypeRepeatable {
				key = mig.Version + "_" + mig.Description
			}

			if a, exists := appliedMap[key]; exists {
				if a.Success {
					entry.Status = "Applied"
					appliedCount++
				} else {
					entry.Status = "Failed"
				}
				entry.AppliedAt = a.AppliedAt
				if mig.Checksum == a.Checksum {
					entry.ChecksumMatch = "OK"
				} else {
					entry.ChecksumMatch = "MISMATCH"
				}
			} else {
				if mig.Type == migration.TypeUndo {
					entry.Status = "Available"
				} else {
					entry.Status = "Pending"
					pendingCount++
				}
				entry.AppliedAt = "-"
				entry.ChecksumMatch = "-"
			}

			entries = append(entries, entry)
		}

		if format == "json" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(entries)
		}

		// Table format
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "VERSION\tDESCRIPTION\tTYPE\tSTATUS\tAPPLIED AT\tCHECKSUM")
		fmt.Fprintln(w, "-------\t-----------\t----\t------\t----------\t--------")

		for _, e := range entries {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				e.Version, e.Description, e.Type, e.Status, e.AppliedAt, e.ChecksumMatch)
		}
		w.Flush()

		fmt.Printf("\nTotal: %d | Applied: %d | Pending: %d\n",
			len(scanned), appliedCount, pendingCount)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
	statusCmd.Flags().String("format", "table", "output format (table, json)")
}
