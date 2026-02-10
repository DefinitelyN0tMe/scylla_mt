package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	versionedPattern  = regexp.MustCompile(`^V(\d+)__(.+)\.(cql|sql)$`)
	undoPattern       = regexp.MustCompile(`^U(\d+)__(.+)\.(cql|sql)$`)
	repeatablePattern = regexp.MustCompile(`^R__(.+)\.(cql|sql)$`)
)

func ScanMigrationsDir(dirPath string) ([]*Migration, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %s: %w", dirPath, err)
	}

	var migrations []*Migration

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		// Skip hidden files (.DS_Store, .gitkeep, etc.)
		if strings.HasPrefix(name, ".") {
			continue
		}

		fullPath := filepath.Join(dirPath, name)

		mig, err := parseMigrationFilename(name, fullPath)
		if err != nil {
			continue // skip non-migration files
		}

		migrations = append(migrations, mig)
	}

	sort.Slice(migrations, func(i, j int) bool {
		mi, mj := migrations[i], migrations[j]

		// Versioned and Undo first, then Repeatable
		if mi.Type == TypeRepeatable && mj.Type != TypeRepeatable {
			return false
		}
		if mi.Type != TypeRepeatable && mj.Type == TypeRepeatable {
			return true
		}
		if mi.Type == TypeRepeatable && mj.Type == TypeRepeatable {
			return mi.Description < mj.Description
		}

		// Sort by version numerically
		cmp := CompareVersions(mi.Version, mj.Version)
		if cmp != 0 {
			return cmp < 0
		}

		// Same version: versioned before undo
		return mi.Type == TypeVersioned
	})

	return migrations, nil
}

func parseMigrationFilename(filename, fullPath string) (*Migration, error) {
	if matches := versionedPattern.FindStringSubmatch(filename); matches != nil {
		return &Migration{
			Version:     matches[1],
			Description: humanize(matches[2]),
			Type:        TypeVersioned,
			Filename:    filename,
			FilePath:    fullPath,
		}, nil
	}

	if matches := undoPattern.FindStringSubmatch(filename); matches != nil {
		return &Migration{
			Version:     matches[1],
			Description: humanize(matches[2]),
			Type:        TypeUndo,
			Filename:    filename,
			FilePath:    fullPath,
		}, nil
	}

	if matches := repeatablePattern.FindStringSubmatch(filename); matches != nil {
		return &Migration{
			Version:     "R",
			Description: humanize(matches[1]),
			Type:        TypeRepeatable,
			Filename:    filename,
			FilePath:    fullPath,
		}, nil
	}

	return nil, fmt.Errorf("filename does not match any migration pattern: %s", filename)
}

func humanize(s string) string {
	return strings.ReplaceAll(s, "_", " ")
}

func GetNextVersion(dirPath string) (int, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}

	maxVersion := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if matches := versionedPattern.FindStringSubmatch(entry.Name()); matches != nil {
			v, err := strconv.Atoi(matches[1])
			if err != nil {
				continue
			}
			if v > maxVersion {
				maxVersion = v
			}
		}
		if matches := undoPattern.FindStringSubmatch(entry.Name()); matches != nil {
			v, err := strconv.Atoi(matches[1])
			if err != nil {
				continue
			}
			if v > maxVersion {
				maxVersion = v
			}
		}
	}

	return maxVersion + 1, nil
}
