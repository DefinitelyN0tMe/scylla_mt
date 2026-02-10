package migration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylla-migrate/scylla-migrate/internal/schema"
)

func TestResolver_GetPendingMigrations_NoneApplied(t *testing.T) {
	dir := t.TempDir()
	createTestMigration(t, dir, "V001__first.cql", "CREATE TABLE first (id UUID PRIMARY KEY);")
	createTestMigration(t, dir, "V002__second.cql", "CREATE TABLE second (id UUID PRIMARY KEY);")

	scanned, err := ScanMigrationsDir(dir)
	require.NoError(t, err)

	resolver := NewResolver(scanned)
	pending, err := resolver.GetPendingMigrations(nil)
	require.NoError(t, err)
	assert.Len(t, pending, 2)
}

func TestResolver_GetPendingMigrations_SomeApplied(t *testing.T) {
	dir := t.TempDir()
	createTestMigration(t, dir, "V001__first.cql", "CREATE TABLE first (id UUID PRIMARY KEY);")
	createTestMigration(t, dir, "V002__second.cql", "CREATE TABLE second (id UUID PRIMARY KEY);")

	scanned, err := ScanMigrationsDir(dir)
	require.NoError(t, err)

	applied := []schema.AppliedMigration{
		{Version: "001", Success: true, Type: "versioned"},
	}

	resolver := NewResolver(scanned)
	pending, err := resolver.GetPendingMigrations(applied)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, "002", pending[0].Version)
}

func TestResolver_GetPendingMigrations_AllApplied(t *testing.T) {
	dir := t.TempDir()
	createTestMigration(t, dir, "V001__first.cql", "CREATE TABLE first (id UUID PRIMARY KEY);")

	scanned, err := ScanMigrationsDir(dir)
	require.NoError(t, err)

	applied := []schema.AppliedMigration{
		{Version: "001", Success: true, Type: "versioned"},
	}

	resolver := NewResolver(scanned)
	pending, err := resolver.GetPendingMigrations(applied)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestResolver_GetUndoMigration(t *testing.T) {
	dir := t.TempDir()
	createTestMigration(t, dir, "V001__create.cql", "CREATE TABLE foo (id UUID PRIMARY KEY);")
	createTestMigration(t, dir, "U001__drop.cql", "DROP TABLE foo;")

	scanned, err := ScanMigrationsDir(dir)
	require.NoError(t, err)

	resolver := NewResolver(scanned)

	undo := resolver.GetUndoMigration("001")
	require.NotNil(t, undo)
	assert.Equal(t, TypeUndo, undo.Type)
	assert.Equal(t, "001", undo.Version)

	// Non-existent version
	assert.Nil(t, resolver.GetUndoMigration("999"))
}

func TestResolver_FilterUpToTarget(t *testing.T) {
	migrations := []*Migration{
		{Version: "001", Type: TypeVersioned},
		{Version: "002", Type: TypeVersioned},
		{Version: "003", Type: TypeVersioned},
		{Version: "R", Type: TypeRepeatable, Description: "views"},
	}

	resolver := NewResolver(nil)
	filtered := resolver.FilterUpToTarget(migrations, "002")

	assert.Len(t, filtered, 3) // 001, 002, and the repeatable
	assert.Equal(t, "001", filtered[0].Version)
	assert.Equal(t, "002", filtered[1].Version)
	assert.Equal(t, TypeRepeatable, filtered[2].Type)
}

func TestResolver_ValidateAppliedChecksums(t *testing.T) {
	dir := t.TempDir()
	createTestMigration(t, dir, "V001__first.cql", "CREATE TABLE first (id UUID PRIMARY KEY);")

	scanned, err := ScanMigrationsDir(dir)
	require.NoError(t, err)

	// Parse to get correct checksum
	require.NoError(t, ParseMigrationFile(scanned[0]))
	correctChecksum := scanned[0].Checksum

	// Valid checksum
	applied := []schema.AppliedMigration{
		{Version: "001", Checksum: correctChecksum, Success: true, Type: "versioned", Description: "first"},
	}

	resolver := NewResolver(scanned)
	errors := resolver.ValidateAppliedChecksums(applied)
	assert.Empty(t, errors)

	// Invalid checksum
	applied[0].Checksum = "invalid_checksum"
	errors = resolver.ValidateAppliedChecksums(applied)
	assert.Len(t, errors, 1)
	assert.Contains(t, errors[0], "checksum mismatch")
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"1", "2", -1},
		{"2", "1", 1},
		{"1", "1", 0},
		{"9", "10", -1},  // numeric: 9 < 10
		{"10", "9", 1},   // numeric: 10 > 9
		{"99", "100", -1},
		{"001", "002", -1},
		{"001", "001", 0},
		{"abc", "def", -1}, // fallback to lexicographic
		{"def", "abc", 1},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got := CompareVersions(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func createTestMigration(t *testing.T, dir, filename, content string) {
	t.Helper()
	path := dir + "/" + filename
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
}
