package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize scylla-migrate project",
	Long:  "Create a configuration file and migrations directory to get started.",
	RunE: func(cmd *cobra.Command, args []string) error {
		initLogger()

		migrationsDir := "./migrations"

		// Create migrations directory
		if err := os.MkdirAll(migrationsDir, 0755); err != nil {
			return fmt.Errorf("failed to create migrations directory: %w", err)
		}
		log.Info().Str("path", migrationsDir).Msg("Created migrations directory")

		// Create config file
		configPath := "./scylla-migrate.yaml"
		if _, err := os.Stat(configPath); err == nil {
			log.Warn().Str("path", configPath).Msg("Config file already exists, skipping")
		} else {
			if err := os.WriteFile(configPath, []byte(configTemplate), 0644); err != nil {
				return fmt.Errorf("failed to create config file: %w", err)
			}
			log.Info().Str("path", configPath).Msg("Created config file")
		}

		// Create example migration
		examplePath := filepath.Join(migrationsDir, "V001__example_migration.cql")
		if _, err := os.Stat(examplePath); err == nil {
			log.Warn().Str("path", examplePath).Msg("Example migration already exists, skipping")
		} else {
			if err := os.WriteFile(examplePath, []byte(exampleMigration), 0644); err != nil {
				return fmt.Errorf("failed to create example migration: %w", err)
			}
			log.Info().Str("path", examplePath).Msg("Created example migration")
		}

		fmt.Println("\nInitialization complete! Next steps:")
		fmt.Println("  1. Edit scylla-migrate.yaml with your cluster settings")
		fmt.Println("  2. Edit or replace migrations/V001__example_migration.cql")
		fmt.Println("  3. Create more migrations: scylla-migrate create <name>")
		fmt.Println("  4. Apply migrations:       scylla-migrate migrate")

		return nil
	},
}

const configTemplate = `# scylla-migrate configuration
# https://github.com/scylla-migrate/scylla-migrate

# ScyllaDB / Cassandra cluster hosts
hosts:
  - "localhost:9042"

# Target keyspace for migrations
keyspace: "my_keyspace"

# Directory containing migration files
migrations_dir: "./migrations"

# Authentication (optional)
username: ""
password: ""

# SSL/TLS configuration (optional)
ssl:
  enabled: false
  ca_cert: ""
  client_cert: ""
  client_key: ""
  skip_verify: false

# Consistency level for migration operations
# Options: one, two, three, quorum, all, local_quorum, each_quorum, local_one
consistency: "quorum"

# Connection timeout
connection_timeout: "10s"

# Query execution timeout
timeout: "30s"

# Lock acquisition timeout for preventing concurrent migrations
lock_timeout: "60s"

# Time to wait for schema agreement across cluster after DDL statements
schema_agreement_timeout: "30s"

# Keyspace used to store migration metadata and locks
metadata_keyspace: "scylla_migrate"

# Replication strategy for the metadata keyspace
metadata_replication:
  class: "SimpleStrategy"
  replication_factor: 1
  # For production with NetworkTopologyStrategy:
  # class: "NetworkTopologyStrategy"
  # datacenters:
  #   dc1: 3
  #   dc2: 3

# Maximum retry attempts for failed operations
max_retries: 3

# CQL native protocol version
protocol_version: 4
`

const exampleMigration = `-- Example Migration
-- Delete or modify this file, then run: scylla-migrate migrate
--
-- This creates a sample table. Replace with your own schema.

CREATE TABLE IF NOT EXISTS my_keyspace.example_users (
    id UUID PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMP
);
`

func init() {
	rootCmd.AddCommand(initCmd)
}
