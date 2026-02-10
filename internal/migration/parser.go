package migration

import (
	"fmt"
	"os"
	"strings"
)

func ParseMigrationFile(mig *Migration) error {
	content, err := os.ReadFile(mig.FilePath)
	if err != nil {
		return fmt.Errorf("failed to read migration file %s: %w", mig.FilePath, err)
	}

	raw := string(content)

	// Strip UTF-8 BOM if present
	raw = strings.TrimPrefix(raw, "\xef\xbb\xbf")

	mig.RawContent = raw

	// Normalize line endings
	raw = strings.ReplaceAll(raw, "\r\n", "\n")

	// Calculate checksum
	checksum, err := CalculateChecksumFromContent([]byte(raw))
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}
	mig.Checksum = checksum

	// Split into statements
	statements, err := splitStatements(raw)
	if err != nil {
		return fmt.Errorf("failed to parse CQL statements in %s: %w", mig.Filename, err)
	}

	mig.Statements = statements
	return nil
}

func splitStatements(content string) ([]string, error) {
	var statements []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false

	runes := []rune(content)
	length := len(runes)

	for i := 0; i < length; i++ {
		ch := runes[i]

		// Handle line comments
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
				current.WriteRune(ch)
			}
			continue
		}

		// Handle block comments
		if inBlockComment {
			if ch == '*' && i+1 < length && runes[i+1] == '/' {
				inBlockComment = false
				i++ // skip '/'
			}
			continue
		}

		// Detect line comment start (--)
		if !inSingleQuote && !inDoubleQuote && ch == '-' && i+1 < length && runes[i+1] == '-' {
			inLineComment = true
			i++ // skip second '-'
			continue
		}

		// Detect block comment start (/*)
		if !inSingleQuote && !inDoubleQuote && ch == '/' && i+1 < length && runes[i+1] == '*' {
			inBlockComment = true
			i++ // skip '*'
			continue
		}

		// Handle string literals
		if !inDoubleQuote && ch == '\'' {
			// Check for escaped quote ('')
			if inSingleQuote && i+1 < length && runes[i+1] == '\'' {
				current.WriteRune(ch)
				current.WriteRune(runes[i+1])
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
			current.WriteRune(ch)
			continue
		}

		if !inSingleQuote && ch == '"' {
			inDoubleQuote = !inDoubleQuote
			current.WriteRune(ch)
			continue
		}

		// Statement separator
		if !inSingleQuote && !inDoubleQuote && ch == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	// Check for unterminated quotes
	if inSingleQuote {
		return nil, fmt.Errorf("unterminated single quote in CQL")
	}
	if inDoubleQuote {
		return nil, fmt.Errorf("unterminated double quote in CQL")
	}
	if inBlockComment {
		return nil, fmt.Errorf("unterminated block comment in CQL")
	}

	// Handle last statement without trailing semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements, nil
}

func IsDDL(statement string) bool {
	upper := strings.ToUpper(strings.TrimSpace(statement))
	return strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "ALTER") ||
		strings.HasPrefix(upper, "DROP")
}
