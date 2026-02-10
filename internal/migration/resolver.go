package migration

import (
	"fmt"
	"sort"

	"github.com/scylla-migrate/scylla-migrate/internal/schema"
)

type Resolver struct {
	migrations []*Migration
}

func NewResolver(migrations []*Migration) *Resolver {
	return &Resolver{migrations: migrations}
}

func (r *Resolver) GetPendingMigrations(applied []schema.AppliedMigration) ([]*Migration, error) {
	appliedMap := make(map[string]schema.AppliedMigration)
	for _, a := range applied {
		if a.Success {
			appliedMap[a.Version] = a
		}
	}

	var pending []*Migration

	for _, mig := range r.migrations {
		switch mig.Type {
		case TypeVersioned:
			if _, exists := appliedMap[mig.Version]; !exists {
				if err := ParseMigrationFile(mig); err != nil {
					return nil, fmt.Errorf("failed to parse migration %s: %w", mig.Filename, err)
				}
				pending = append(pending, mig)
			}
		case TypeRepeatable:
			if err := ParseMigrationFile(mig); err != nil {
				return nil, fmt.Errorf("failed to parse migration %s: %w", mig.Filename, err)
			}
			key := mig.Version + "_" + mig.Description
			if a, exists := appliedMap[key]; !exists {
				pending = append(pending, mig)
			} else if a.Checksum != mig.Checksum {
				pending = append(pending, mig)
			}
		case TypeUndo:
			continue
		}
	}

	return pending, nil
}

func (r *Resolver) ValidateAppliedChecksums(applied []schema.AppliedMigration) []string {
	var errors []string

	fileMap := make(map[string]*Migration)
	for _, mig := range r.migrations {
		if mig.Type == TypeVersioned {
			fileMap[mig.Version] = mig
		}
	}

	for _, a := range applied {
		if !a.Success || a.Type == "repeatable" {
			continue
		}

		fileMig, exists := fileMap[a.Version]
		if !exists {
			errors = append(errors, fmt.Sprintf(
				"applied migration V%s (%s) has no corresponding file",
				a.Version, a.Description,
			))
			continue
		}

		if err := ParseMigrationFile(fileMig); err != nil {
			errors = append(errors, fmt.Sprintf(
				"failed to parse V%s (%s): %s",
				a.Version, a.Description, err,
			))
			continue
		}

		if fileMig.Checksum != a.Checksum {
			errors = append(errors, fmt.Sprintf(
				"checksum mismatch for V%s (%s): recorded=%s, current=%s",
				a.Version, a.Description, a.Checksum, fileMig.Checksum,
			))
		}
	}

	return errors
}

func (r *Resolver) GetVersionedMigrations() []*Migration {
	var versioned []*Migration
	for _, mig := range r.migrations {
		if mig.Type == TypeVersioned {
			versioned = append(versioned, mig)
		}
	}
	sort.Slice(versioned, func(i, j int) bool {
		return CompareVersions(versioned[i].Version, versioned[j].Version) < 0
	})
	return versioned
}

func (r *Resolver) GetUndoMigration(version string) *Migration {
	for _, mig := range r.migrations {
		if mig.Type == TypeUndo && mig.Version == version {
			return mig
		}
	}
	return nil
}

func (r *Resolver) FilterUpToTarget(migrations []*Migration, target string) []*Migration {
	var filtered []*Migration
	for _, mig := range migrations {
		if mig.Type == TypeRepeatable {
			filtered = append(filtered, mig)
			continue
		}
		if CompareVersions(mig.Version, target) <= 0 {
			filtered = append(filtered, mig)
		}
	}
	return filtered
}
