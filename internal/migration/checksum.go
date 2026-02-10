package migration

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
)

func CalculateChecksum(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for checksum: %w", err)
	}

	return CalculateChecksumFromContent(content)
}

func CalculateChecksumFromContent(content []byte) (string, error) {
	// Normalize CRLF to LF for consistent checksums across platforms
	normalized := strings.ReplaceAll(string(content), "\r\n", "\n")

	hash := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", hash), nil
}
