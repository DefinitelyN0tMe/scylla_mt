package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name:  "single statement",
			input: "CREATE TABLE foo (id UUID PRIMARY KEY);",
			want:  []string{"CREATE TABLE foo (id UUID PRIMARY KEY)"},
		},
		{
			name:  "multiple statements",
			input: "CREATE TABLE foo (id UUID PRIMARY KEY);\nCREATE INDEX ON foo (name);",
			want: []string{
				"CREATE TABLE foo (id UUID PRIMARY KEY)",
				"CREATE INDEX ON foo (name)",
			},
		},
		{
			name:  "statement with string containing semicolons",
			input: "INSERT INTO foo (id, name) VALUES (uuid(), 'hello; world');",
			want:  []string{"INSERT INTO foo (id, name) VALUES (uuid(), 'hello; world')"},
		},
		{
			name:  "statement with line comments",
			input: "-- This is a comment\nCREATE TABLE foo (id UUID PRIMARY KEY);",
			want:  []string{"CREATE TABLE foo (id UUID PRIMARY KEY)"},
		},
		{
			name:  "statement with block comments",
			input: "/* Block comment */ CREATE TABLE foo (id UUID PRIMARY KEY);",
			want:  []string{"CREATE TABLE foo (id UUID PRIMARY KEY)"},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "only comments",
			input: "-- just a comment\n/* another one */",
			want:  nil,
		},
		{
			name:  "escaped single quotes",
			input: "INSERT INTO foo (name) VALUES ('it''s a test');",
			want:  []string{"INSERT INTO foo (name) VALUES ('it''s a test')"},
		},
		{
			name:  "no trailing semicolon",
			input: "CREATE TABLE foo (id UUID PRIMARY KEY)",
			want:  []string{"CREATE TABLE foo (id UUID PRIMARY KEY)"},
		},
		{
			name:    "unterminated string",
			input:   "INSERT INTO foo VALUES ('unterminated);",
			wantErr: true,
		},
		{
			name:    "unterminated block comment",
			input:   "/* unterminated comment CREATE TABLE foo;",
			wantErr: true,
		},
		{
			name:  "multiline create table",
			input: "CREATE TABLE foo (\n    id UUID,\n    name TEXT,\n    PRIMARY KEY (id)\n);",
			want:  []string{"CREATE TABLE foo (\n    id UUID,\n    name TEXT,\n    PRIMARY KEY (id)\n)"},
		},
		{
			name: "double-quoted identifiers with semicolons",
			input: `CREATE TABLE "my;table" (id UUID PRIMARY KEY);`,
			want:  []string{`CREATE TABLE "my;table" (id UUID PRIMARY KEY)`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitStatements(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseMigrationFile(t *testing.T) {
	dir := t.TempDir()
	content := `-- Migration: create users
CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);
CREATE INDEX ON users (name);
`
	path := filepath.Join(dir, "V001__create_users.cql")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	mig := &Migration{
		Version:  "001",
		Filename: "V001__create_users.cql",
		FilePath: path,
		Type:     TypeVersioned,
	}

	err := ParseMigrationFile(mig)
	require.NoError(t, err)

	assert.Len(t, mig.Statements, 2)
	assert.Equal(t, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", mig.Statements[0])
	assert.Equal(t, "CREATE INDEX ON users (name)", mig.Statements[1])
	assert.NotEmpty(t, mig.Checksum)
}

func TestIsDDL(t *testing.T) {
	assert.True(t, IsDDL("CREATE TABLE foo (id UUID PRIMARY KEY)"))
	assert.True(t, IsDDL("ALTER TABLE foo ADD name TEXT"))
	assert.True(t, IsDDL("DROP TABLE foo"))
	assert.True(t, IsDDL("  CREATE INDEX ON foo (name)"))
	assert.False(t, IsDDL("INSERT INTO foo VALUES (1, 'test')"))
	assert.False(t, IsDDL("SELECT * FROM foo"))
	assert.False(t, IsDDL("UPDATE foo SET name = 'test'"))
}
