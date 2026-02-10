package migration

import "strconv"

type MigrationType string

const (
	TypeVersioned  MigrationType = "versioned"
	TypeUndo       MigrationType = "undo"
	TypeRepeatable MigrationType = "repeatable"
)

type Migration struct {
	Version     string
	Description string
	Type        MigrationType
	Filename    string
	FilePath    string
	Checksum    string
	Statements  []string
	RawContent  string
}

// CompareVersions compares two version strings numerically.
// Returns -1, 0, or 1.
func CompareVersions(a, b string) int {
	ai, errA := strconv.Atoi(a)
	bi, errB := strconv.Atoi(b)

	// If both parse as integers, compare numerically
	if errA == nil && errB == nil {
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	}

	// Fallback to lexicographic for non-numeric versions
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
