package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/scylla-migrate/scylla-migrate/internal/config"
)

var (
	cfgFile string
	cfg     *config.Config
	log     zerolog.Logger

	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "scylla-migrate",
	Short: "Database migration tool for ScyllaDB and Apache Cassandra",
	Long: `scylla-migrate is a production-ready schema migration tool for ScyllaDB and Apache Cassandra.

It provides versioned migrations, rollback support, distributed locking,
checksum validation, and a familiar Flyway-like workflow.

Migration file naming convention:
  V<version>__<description>.cql    Versioned migration
  U<version>__<description>.cql    Undo (rollback) migration
  R__<description>.cql             Repeatable migration`,
	Version:       version,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: ./scylla-migrate.yaml)")
	rootCmd.PersistentFlags().StringSlice("hosts", nil, "ScyllaDB hosts (comma-separated)")
	rootCmd.PersistentFlags().String("keyspace", "", "target keyspace")
	rootCmd.PersistentFlags().String("migrations-dir", "", "migrations directory (default: ./migrations)")
	rootCmd.PersistentFlags().String("username", "", "authentication username")
	rootCmd.PersistentFlags().String("password", "", "authentication password")
	rootCmd.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")

	_ = viper.BindPFlag("hosts", rootCmd.PersistentFlags().Lookup("hosts"))
	_ = viper.BindPFlag("keyspace", rootCmd.PersistentFlags().Lookup("keyspace"))
	_ = viper.BindPFlag("migrations_dir", rootCmd.PersistentFlags().Lookup("migrations-dir"))
	_ = viper.BindPFlag("username", rootCmd.PersistentFlags().Lookup("username"))
	_ = viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))

	rootCmd.SetVersionTemplate(fmt.Sprintf("scylla-migrate %s (commit: %s, built: %s)\n", version, commit, date))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("scylla-migrate")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.scylla-migrate")
		viper.AddConfigPath("/etc/scylla-migrate")
	}

	viper.SetEnvPrefix("SCYLLA_MIGRATE")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

func initLogger() {
	level := viper.GetString("log_level")
	if level == "" {
		level = "info"
	}

	var l zerolog.Level
	switch level {
	case "debug":
		l = zerolog.DebugLevel
	case "warn":
		l = zerolog.WarnLevel
	case "error":
		l = zerolog.ErrorLevel
	default:
		l = zerolog.InfoLevel
	}

	log = zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05",
	}).Level(l).With().Timestamp().Logger()
}

func loadConfig() error {
	initLogger()

	var err error
	cfg, err = config.Load()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	return nil
}
