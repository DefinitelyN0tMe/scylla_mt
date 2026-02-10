package schema

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/scylla-migrate/scylla-migrate/internal/driver"
)

type AppliedMigration struct {
	Version         string
	Description     string
	Type            string
	Script          string
	Checksum        string
	AppliedBy       string
	AppliedAt       time.Time
	ExecutionTimeMS int
	Success         bool
}

type MigrationRecord struct {
	Version     string
	Description string
	Type        string
	Filename    string
	Checksum    string
}

type MetadataManager struct {
	session  *driver.Session
	keyspace string
	Logger   zerolog.Logger
}

func NewMetadataManager(session *driver.Session, keyspace string, logger zerolog.Logger) *MetadataManager {
	return &MetadataManager{
		session:  session,
		keyspace: keyspace,
		Logger:   logger,
	}
}

func (m *MetadataManager) GetAppliedMigrations() ([]AppliedMigration, error) {
	query := fmt.Sprintf(
		`SELECT version, description, type, script, checksum, applied_by, applied_at, execution_time_ms, success
		 FROM %s.schema_migrations`,
		m.keyspace,
	)

	iter := m.session.Query(query).Iter()
	var applied []AppliedMigration

	var a AppliedMigration
	for iter.Scan(
		&a.Version, &a.Description, &a.Type, &a.Script, &a.Checksum,
		&a.AppliedBy, &a.AppliedAt, &a.ExecutionTimeMS, &a.Success,
	) {
		applied = append(applied, a)
		a = AppliedMigration{}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}

	sort.Slice(applied, func(i, j int) bool {
		ai, errA := strconv.Atoi(applied[i].Version)
		bi, errB := strconv.Atoi(applied[j].Version)
		if errA == nil && errB == nil {
			return ai < bi
		}
		return applied[i].Version < applied[j].Version
	})

	return applied, nil
}

func (m *MetadataManager) RecordMigration(rec MigrationRecord, executionTime time.Duration, success bool, hostname string) error {
	query := fmt.Sprintf(
		`INSERT INTO %s.schema_migrations
		 (version, description, type, script, checksum, applied_by, applied_at, execution_time_ms, success)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.keyspace,
	)

	return m.session.Execute(query,
		rec.Version,
		rec.Description,
		rec.Type,
		rec.Filename,
		rec.Checksum,
		hostname,
		time.Now(),
		int(executionTime.Milliseconds()),
		success,
	)
}

func (m *MetadataManager) RemoveMigration(version string) error {
	query := fmt.Sprintf(
		`DELETE FROM %s.schema_migrations WHERE version = ?`,
		m.keyspace,
	)
	return m.session.Execute(query, version)
}

func (m *MetadataManager) UpdateChecksum(version, newChecksum string) error {
	query := fmt.Sprintf(
		`UPDATE %s.schema_migrations SET checksum = ? WHERE version = ?`,
		m.keyspace,
	)
	return m.session.Execute(query, newChecksum, version)
}

func (m *MetadataManager) GetLastAppliedVersion() (string, error) {
	applied, err := m.GetAppliedMigrations()
	if err != nil {
		return "", err
	}

	lastVersion := ""
	lastNum := -1
	for _, a := range applied {
		if a.Success && a.Type == "versioned" {
			num, err := strconv.Atoi(a.Version)
			if err != nil {
				// Non-numeric: fallback to lexicographic
				if a.Version > lastVersion {
					lastVersion = a.Version
				}
				continue
			}
			if num > lastNum {
				lastNum = num
				lastVersion = a.Version
			}
		}
	}

	return lastVersion, nil
}

func (m *MetadataManager) GetFailedMigrations() ([]AppliedMigration, error) {
	applied, err := m.GetAppliedMigrations()
	if err != nil {
		return nil, err
	}

	var failed []AppliedMigration
	for _, a := range applied {
		if !a.Success {
			failed = append(failed, a)
		}
	}

	return failed, nil
}
