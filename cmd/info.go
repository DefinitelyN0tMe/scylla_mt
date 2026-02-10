package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Show cluster and migration info",
	Long:  "Display current schema version, cluster details, and configuration summary.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		ctx, err := migration.NewExecutionContext(cfg, log)
		if err != nil {
			return err
		}
		defer ctx.Close()

		metadata, err := ctx.Session.GetClusterMetadata()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get cluster metadata")
		}

		lastVersion, err := ctx.MetadataManager.GetLastAppliedVersion()
		if err != nil {
			lastVersion = "none"
		}
		if lastVersion == "" {
			lastVersion = "none"
		}

		fmt.Printf("scylla-migrate %s\n\n", version)

		fmt.Println("Cluster:")
		if metadata != nil {
			fmt.Printf("  Name:           %s\n", metadata.ClusterName)
			fmt.Printf("  Schema Version: %s\n", metadata.SchemaVer)
		}
		fmt.Printf("  Hosts:          %v\n", cfg.Hosts)
		fmt.Printf("  Keyspace:       %s\n", cfg.Keyspace)

		fmt.Println("\nMigration:")
		fmt.Printf("  Directory:      %s\n", cfg.MigrationsDir)
		fmt.Printf("  Metadata:       %s\n", cfg.MetadataKeyspace)
		fmt.Printf("  Current:        V%s\n", lastVersion)

		fmt.Println("\nSettings:")
		fmt.Printf("  Consistency:    %s\n", cfg.Consistency)
		fmt.Printf("  Timeout:        %s\n", cfg.Timeout)
		fmt.Printf("  Lock Timeout:   %s\n", cfg.LockTimeout)
		fmt.Printf("  Schema Agree:   %s\n", cfg.SchemaAgreementTimeout)
		fmt.Printf("  SSL:            %v\n", cfg.SSL.Enabled)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
