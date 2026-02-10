package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanMigrationsDir(t *testing.T) {
	dir := filepath.Join("..", "..", "testdata", "migrations")

	migrations, err := ScanMigrationsDir(dir)
	require.NoError(t, err)
	require.Len(t, migrations, 4)

	// Versioned and undo first, then repeatable
	assert.Equal(t, "001", migrations[0].Version)
	assert.Equal(t, TypeVersioned, migrations[0].Type)
	assert.Equal(t, "create users", migrations[0].Description)

	assert.Equal(t, "001", migrations[1].Version)
	assert.Equal(t, TypeUndo, migrations[1].Type)

	assert.Equal(t, "002", migrations[2].Version)
	assert.Equal(t, TypeVersioned, migrations[2].Type)

	assert.Equal(t, "R", migrations[3].Version)
	assert.Equal(t, TypeRepeatable, migrations[3].Type)
}

func TestScanMigrationsDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	migrations, err := ScanMigrationsDir(dir)
	require.NoError(t, err)
	assert.Empty(t, migrations)
}

func TestScanMigrationsDir_NonExistentDir(t *testing.T) {
	_, err := ScanMigrationsDir("/nonexistent/path")
	assert.Error(t, err)
}

func TestParseMigrationFilename(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		wantVersion string
		wantType    MigrationType
		wantDesc    string
		wantErr     bool
	}{
		{
			name:        "versioned migration",
			filename:    "V001__create_users_table.cql",
			wantVersion: "001",
			wantType:    TypeVersioned,
			wantDesc:    "create users table",
		},
		{
			name:        "undo migration",
			filename:    "U001__drop_users_table.cql",
			wantVersion: "001",
			wantType:    TypeUndo,
			wantDesc:    "drop users table",
		},
		{
			name:        "repeatable migration",
			filename:    "R__refresh_views.cql",
			wantVersion: "R",
			wantType:    TypeRepeatable,
			wantDesc:    "refresh views",
		},
		{
			name:        "sql extension",
			filename:    "V002__add_index.sql",
			wantVersion: "002",
			wantType:    TypeVersioned,
			wantDesc:    "add index",
		},
		{
			name:     "invalid filename",
			filename: "readme.txt",
			wantErr:  true,
		},
		{
			name:     "no version",
			filename: "V__missing_version.cql",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mig, err := parseMigrationFilename(tt.filename, "/test/"+tt.filename)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, mig.Version)
			assert.Equal(t, tt.wantType, mig.Type)
			assert.Equal(t, tt.wantDesc, mig.Description)
		})
	}
}

func TestGetNextVersion(t *testing.T) {
	dir := t.TempDir()

	// Empty dir
	v, err := GetNextVersion(dir)
	require.NoError(t, err)
	assert.Equal(t, 1, v)

	// After creating some migrations
	os.WriteFile(filepath.Join(dir, "V001__first.cql"), []byte("test"), 0644)
	os.WriteFile(filepath.Join(dir, "V003__third.cql"), []byte("test"), 0644)

	v, err = GetNextVersion(dir)
	require.NoError(t, err)
	assert.Equal(t, 4, v)
}
