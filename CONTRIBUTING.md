# Contributing to scylla-migrate

Thank you for your interest in contributing!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/scylla-migrate.git`
3. Create a branch: `git checkout -b feature/my-feature`
4. Make your changes
5. Run tests: `make test`
6. Run linter: `make lint`
7. Commit and push
8. Open a Pull Request

## Development Setup

### Prerequisites

- Go 1.21+
- Docker (for integration tests)
- golangci-lint (for linting)

### Running Tests

```bash
# Unit tests
make test

# Integration tests (requires ScyllaDB via Docker)
make docker-test

# Coverage report
make test-coverage
```

### Project Structure

```
cmd/           CLI commands (cobra)
internal/      Internal packages (not importable externally)
  config/      Configuration loading and validation
  driver/      ScyllaDB session management
  migration/   Core migration logic (parse, resolve, execute)
  lock/        Distributed locking via LWT
  schema/      Migration metadata table management
pkg/migrate/   Public API for library usage
testdata/      Test fixtures
```

## Guidelines

- Follow existing code patterns and Go idioms
- Write tests for new functionality
- Keep commits focused and well-described
- Update documentation if changing user-facing behavior

## Reporting Issues

Please use [GitHub Issues](https://github.com/scylla-migrate/scylla-migrate/issues) with:
- scylla-migrate version (`scylla-migrate --version`)
- ScyllaDB / Cassandra version
- Steps to reproduce
- Expected vs actual behavior

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
