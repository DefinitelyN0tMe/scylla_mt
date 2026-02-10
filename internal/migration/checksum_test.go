package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateChecksum(t *testing.T) {
	dir := t.TempDir()
	content := "CREATE TABLE foo (id UUID PRIMARY KEY);\n"

	path := filepath.Join(dir, "test.cql")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	checksum, err := CalculateChecksum(path)
	require.NoError(t, err)
	assert.NotEmpty(t, checksum)
	assert.Len(t, checksum, 64) // SHA-256 hex
}

func TestCalculateChecksum_CRLFNormalization(t *testing.T) {
	// Same content with different line endings should produce same checksum
	lfContent := []byte("CREATE TABLE foo (\n    id UUID PRIMARY KEY\n);\n")
	crlfContent := []byte("CREATE TABLE foo (\r\n    id UUID PRIMARY KEY\r\n);\r\n")

	lfChecksum, err := CalculateChecksumFromContent(lfContent)
	require.NoError(t, err)

	crlfChecksum, err := CalculateChecksumFromContent(crlfContent)
	require.NoError(t, err)

	assert.Equal(t, lfChecksum, crlfChecksum)
}

func TestCalculateChecksum_DifferentContent(t *testing.T) {
	content1 := []byte("CREATE TABLE foo (id UUID PRIMARY KEY);")
	content2 := []byte("CREATE TABLE bar (id UUID PRIMARY KEY);")

	c1, err := CalculateChecksumFromContent(content1)
	require.NoError(t, err)

	c2, err := CalculateChecksumFromContent(content2)
	require.NoError(t, err)

	assert.NotEqual(t, c1, c2)
}

func TestCalculateChecksum_Deterministic(t *testing.T) {
	content := []byte("CREATE TABLE foo (id UUID PRIMARY KEY);")

	c1, _ := CalculateChecksumFromContent(content)
	c2, _ := CalculateChecksumFromContent(content)

	assert.Equal(t, c1, c2)
}
