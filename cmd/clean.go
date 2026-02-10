package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/driver"
)

var cleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "Drop all objects in the configured keyspace",
	Long: `WARNING: This is a destructive operation!

Drops the configured keyspace and all its data, along with the migration
metadata keyspace. Requires the --force flag and interactive confirmation.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		force, _ := cmd.Flags().GetBool("force")
		if !force {
			return fmt.Errorf("this is a destructive operation — use --force to proceed")
		}

		// Interactive confirmation
		fmt.Printf("WARNING: This will DROP keyspace '%s' and ALL its data!\n", cfg.Keyspace)
		fmt.Printf("It will also DROP the metadata keyspace '%s'.\n\n", cfg.MetadataKeyspace)
		fmt.Printf("Type the keyspace name '%s' to confirm: ", cfg.Keyspace)

		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(response)

		if response != cfg.Keyspace {
			return fmt.Errorf("keyspace name does not match — aborting")
		}

		session, err := driver.NewSession(cfg, log)
		if err != nil {
			return err
		}
		defer session.Close()

		// Drop target keyspace
		log.Warn().Str("keyspace", cfg.Keyspace).Msg("Dropping keyspace")
		if err := session.Execute(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", cfg.Keyspace)); err != nil {
			return fmt.Errorf("failed to drop keyspace %s: %w", cfg.Keyspace, err)
		}
		if err := session.WaitForSchemaAgreement(cfg.SchemaAgreementTimeout); err != nil {
			log.Warn().Err(err).Msg("Schema agreement timeout after dropping keyspace")
		}
		log.Info().Str("keyspace", cfg.Keyspace).Msg("Keyspace dropped")

		// Drop metadata keyspace
		log.Warn().Str("keyspace", cfg.MetadataKeyspace).Msg("Dropping metadata keyspace")
		if err := session.Execute(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", cfg.MetadataKeyspace)); err != nil {
			return fmt.Errorf("failed to drop metadata keyspace %s: %w", cfg.MetadataKeyspace, err)
		}
		if err := session.WaitForSchemaAgreement(cfg.SchemaAgreementTimeout); err != nil {
			log.Warn().Err(err).Msg("Schema agreement timeout after dropping metadata keyspace")
		}
		log.Info().Str("keyspace", cfg.MetadataKeyspace).Msg("Metadata keyspace dropped")

		log.Info().Msg("Clean complete — all migration data has been removed")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(cleanCmd)
	cleanCmd.Flags().Bool("force", false, "required flag to confirm destructive operation")
}
