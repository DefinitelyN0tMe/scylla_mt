package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/scylla-migrate/scylla-migrate/internal/migration"
)

var createCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create new migration files",
	Long:  "Generate migration file scaffolding with auto-incremented version number.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(); err != nil {
			return err
		}

		name := args[0]
		withUndo, _ := cmd.Flags().GetBool("with-undo")
		repeatable, _ := cmd.Flags().GetBool("repeatable")

		migrationsDir := cfg.MigrationsDir
		if err := os.MkdirAll(migrationsDir, 0755); err != nil {
			return fmt.Errorf("failed to create migrations directory: %w", err)
		}

		sanitized := sanitizeName(name)
		timestamp := time.Now().Format("2006-01-02 15:04:05")

		var files []string

		if repeatable {
			filename := fmt.Sprintf("R__%s.cql", sanitized)
			path := filepath.Join(migrationsDir, filename)
			content := fmt.Sprintf(`-- Repeatable Migration: %s
-- Created: %s
--
-- This migration runs every time its content changes.
-- Write idempotent CQL statements below.

`, name, timestamp)

			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}
			files = append(files, path)
		} else {
			nextVersion, err := migration.GetNextVersion(migrationsDir)
			if err != nil {
				return fmt.Errorf("failed to determine next version: %w", err)
			}

			// Versioned migration
			filename := fmt.Sprintf("V%03d__%s.cql", nextVersion, sanitized)
			path := filepath.Join(migrationsDir, filename)
			content := fmt.Sprintf(`-- Migration: %s
-- Version: %03d
-- Created: %s

`, name, nextVersion, timestamp)

			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}
			files = append(files, path)

			// Undo migration
			if withUndo {
				undoFilename := fmt.Sprintf("U%03d__%s.cql", nextVersion, sanitized)
				undoPath := filepath.Join(migrationsDir, undoFilename)
				undoContent := fmt.Sprintf(`-- Undo Migration: %s
-- Version: %03d
-- Created: %s
--
-- This script reverses the changes made by V%03d__%s.cql

`, name, nextVersion, timestamp, nextVersion, sanitized)

				if err := os.WriteFile(undoPath, []byte(undoContent), 0644); err != nil {
					return fmt.Errorf("failed to create undo file: %w", err)
				}
				files = append(files, undoPath)
			}
		}

		for _, f := range files {
			log.Info().Str("file", f).Msg("Created migration file")
		}

		return nil
	},
}

func sanitizeName(name string) string {
	s := strings.ToLower(name)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")

	var result strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.Flags().Bool("with-undo", false, "also create an undo migration file")
	createCmd.Flags().Bool("repeatable", false, "create a repeatable migration (no version number)")
}
