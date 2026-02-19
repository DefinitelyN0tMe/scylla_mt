# scylla-migrate

A production-ready schema migration tool for **ScyllaDB** and **Apache Cassandra**.
Like [Flyway](https://flywaydb.org/) — but for CQL.

- Single binary, zero dependencies
- Versioned + undo + repeatable migrations
- Distributed locking (prevents concurrent migrations across nodes)
- Checksum validation (detects tampered migration files)
- Schema agreement waiting (safe for multi-node clusters)
- Dry-run mode
- Library API for embedding in Go applications

## Installation

### Go install

```bash
go install github.com/DefinitelyN0tMe/scylla-migrate@latest
```

### Docker

```bash
docker run --rm -v $(pwd):/app -w /app ghcr.io/DefinitelyN0tMe/scylla-migrate migrate
```

### From source

```bash
git clone https://github.com/DefinitelyN0tMe/scylla-migrate.git
cd scylla-migrate
make build
```

## Quick Start

```bash
# 1. Initialize project
scylla-migrate init

# 2. Edit scylla-migrate.yaml with your cluster settings
#    (hosts, keyspace, credentials)

# 3. Create a migration
scylla-migrate create create_users_table --with-undo

# 4. Edit the generated file: migrations/V001__create_users_table.cql
# 5. Apply
scylla-migrate migrate

# 6. Check status
scylla-migrate status
```

## Migration Files

### Naming Convention

| Prefix | Pattern | Description |
|--------|---------|-------------|
| `V` | `V<version>__<description>.cql` | Versioned migration (applied once) |
| `U` | `U<version>__<description>.cql` | Undo migration (for rollback) |
| `R` | `R__<description>.cql` | Repeatable (re-applied when content changes) |

Examples:
```
migrations/
  V001__create_users_table.cql
  V002__add_email_index.cql
  V003__create_orders_table.cql
  U001__drop_users_table.cql
  U002__remove_email_index.cql
  R__refresh_materialized_views.cql
```

- Version numbers are zero-padded integers (001, 002, ...)
- Double underscore `__` separates version from description
- Both `.cql` and `.sql` extensions are supported
- Files can contain multiple CQL statements separated by `;`

### Writing Migrations

```sql
-- V001__create_users_table.cql

CREATE TABLE my_keyspace.users (
    id UUID PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS users_email_idx ON my_keyspace.users (email);
```

```sql
-- U001__drop_users_table.cql

DROP INDEX IF EXISTS my_keyspace.users_email_idx;
DROP TABLE IF EXISTS my_keyspace.users;
```

## CLI Reference

### `scylla-migrate init`
Create config file and migrations directory.

### `scylla-migrate create <name>`
Generate migration file(s) with auto-incremented version.

```bash
scylla-migrate create add_orders_table           # V002__add_orders_table.cql
scylla-migrate create add_orders_table --with-undo  # + U002__add_orders_table.cql
scylla-migrate create refresh_views --repeatable    # R__refresh_views.cql
```

### `scylla-migrate migrate`
Apply all pending migrations.

```bash
scylla-migrate migrate                    # apply all pending
scylla-migrate migrate --target 003       # apply up to V003
scylla-migrate migrate --dry-run          # preview without applying
```

### `scylla-migrate rollback`
Rollback migrations using undo scripts.

```bash
scylla-migrate rollback                   # rollback last migration
scylla-migrate rollback --steps 3         # rollback last 3
scylla-migrate rollback --to 001          # rollback to V001
scylla-migrate rollback --dry-run         # preview rollback
```

### `scylla-migrate status`
Show migration status table.

```bash
scylla-migrate status                     # table format
scylla-migrate status --format json       # JSON output
```

### `scylla-migrate validate`
Verify checksums of applied migrations haven't changed.

### `scylla-migrate repair`
Fix migration metadata.

```bash
scylla-migrate repair --recalculate-checksums   # update checksums
scylla-migrate repair --remove-failed           # remove failed records
```

### `scylla-migrate info`
Display cluster and migration information.

### `scylla-migrate clean --force`
Drop the configured keyspace and all data. Requires `--force` and interactive confirmation.

### Global Flags

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--config` | — | Config file path |
| `--hosts` | `SCYLLA_MIGRATE_HOSTS` | Cluster hosts (comma-separated) |
| `--keyspace` | `SCYLLA_MIGRATE_KEYSPACE` | Target keyspace |
| `--migrations-dir` | `SCYLLA_MIGRATE_MIGRATIONS_DIR` | Migrations directory |
| `--username` | `SCYLLA_MIGRATE_USERNAME` | Auth username |
| `--password` | `SCYLLA_MIGRATE_PASSWORD` | Auth password |
| `--log-level` | `SCYLLA_MIGRATE_LOG_LEVEL` | Log level (debug/info/warn/error) |

## Configuration

Configuration is loaded from (highest priority first):

1. CLI flags
2. Environment variables (`SCYLLA_MIGRATE_*`)
3. Config file (`scylla-migrate.yaml`)
4. Defaults

### Config File

```yaml
hosts:
  - "localhost:9042"

keyspace: "my_app"
migrations_dir: "./migrations"

# Authentication
username: ""
password: ""

# SSL/TLS
ssl:
  enabled: false
  ca_cert: ""
  client_cert: ""
  client_key: ""

# Tuning
consistency: "quorum"
timeout: "30s"
connection_timeout: "10s"
lock_timeout: "60s"
schema_agreement_timeout: "30s"

# Metadata
metadata_keyspace: "scylla_migrate"
metadata_replication:
  class: "SimpleStrategy"
  replication_factor: 1
  # For production:
  # class: "NetworkTopologyStrategy"
  # datacenters:
  #   dc1: 3

max_retries: 3
protocol_version: 4
```

## Library Usage

Embed migrations in your Go application:

```go
package main

import (
    "log"
    "github.com/scylla-migrate/scylla-migrate/pkg/migrate"
)

func main() {
    m, err := migrate.New(
        migrate.WithHosts("node1:9042", "node2:9042"),
        migrate.WithKeyspace("my_app"),
        migrate.WithMigrationsDir("./migrations"),
        migrate.WithAuth("user", "pass"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer m.Close()

    if err := m.Migrate(); err != nil {
        log.Fatal(err)
    }
}
```

## How It Works

### Migration Tracking

scylla-migrate stores metadata in a dedicated keyspace (`scylla_migrate` by default):

- **`schema_migrations`** — Records every applied migration with version, checksum, timestamp, and execution duration.
- **`schema_lock`** — Distributed lock using Lightweight Transactions (LWT) to prevent concurrent migrations.

### Distributed Locking

When you run `migrate`, the tool:

1. Acquires a cluster-wide lock using CQL `INSERT ... IF NOT EXISTS` (LWT)
2. Applies all pending migrations sequentially
3. Waits for schema agreement after each DDL statement
4. Releases the lock

If another process is running migrations, your command will wait (up to `lock_timeout`) and retry with exponential backoff.

### Schema Agreement

After every DDL statement (CREATE, ALTER, DROP), scylla-migrate waits for all cluster nodes to agree on the new schema version. This prevents read-your-writes issues in multi-node deployments.

### Checksum Validation

Before applying new migrations, scylla-migrate verifies that previously applied migration files haven't been modified (by comparing SHA-256 checksums). This catches accidental edits to already-applied migrations.

## Best Practices

1. **Never modify applied migrations** — Create a new migration instead.
2. **Use `IF NOT EXISTS` / `IF EXISTS`** — Makes migrations safer for retry scenarios.
3. **One logical change per migration** — Easier to debug and rollback.
4. **Always create undo scripts** for production migrations (`--with-undo`).
5. **Test migrations** in a staging environment before production.
6. **Use `--dry-run`** to preview changes before applying.
7. **Set `NetworkTopologyStrategy`** for production metadata keyspace replication.

## Production Safety

This section covers important operational concerns for running scylla-migrate in production environments.

### Metadata Keyspace Replication

By default, scylla-migrate creates its metadata keyspace (`scylla_migrate`) with `SimpleStrategy` and `replication_factor: 1`. **This is intended for development only.**

In production, you **must** configure `NetworkTopologyStrategy` with an appropriate replication factor for your cluster topology:

```yaml
metadata_replication:
  class: "NetworkTopologyStrategy"
  datacenters:
    dc1: 3
    dc2: 3
```

**Why this matters:**
- With RF=1, losing a single node means losing migration metadata — the tool will not know which migrations have been applied.
- LWT-based distributed locking requires a quorum, which is impossible with RF=1 if the replica node goes down.
- If the metadata becomes unavailable, all future migrations will be blocked until the node is restored.

> **Recommendation:** Set the replication factor to at least 3 per datacenter (or match your application keyspace's replication strategy).

### Rollback Limitations

Rollbacks in CQL/ScyllaDB are fundamentally different from SQL databases:

- **DDL statements are not transactional.** If a rollback fails midway (e.g., network error after `DROP TABLE` but before `DROP INDEX`), the schema will be in a partially rolled-back state. You'll need to manually finish or fix it.
- **Data loss is irreversible.** `DROP TABLE` permanently deletes all data in that table. There is no automatic backup before rollback — plan your own backup strategy.
- **Undo scripts must be written manually.** scylla-migrate generates empty `U<version>__<description>.cql` files with `--with-undo`. It's your responsibility to write correct undo CQL.
- **Undo migrations are not tracked in metadata.** When rollback executes undo statements, it removes the corresponding versioned migration record but does not create a new entry. This means rollbacks won't appear in `scylla-migrate status` history.

> **Recommendation:** Always test rollback scripts in staging first. For critical production tables, take a snapshot (`nodetool snapshot`) before running rollbacks.

### `IF EXISTS` / `IF NOT EXISTS` in Migrations

Always use guard clauses in your CQL statements:

```sql
-- Good: safe for retries
CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY, name TEXT);
CREATE INDEX IF NOT EXISTS users_name_idx ON users (name);

-- Dangerous: fails on retry if table already exists
CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);
```

**Why this matters:**
- If a migration partially succeeds (e.g., the first statement runs, then a network timeout occurs before metadata is recorded), re-running `scylla-migrate migrate` will re-execute all statements in that migration.
- Without `IF NOT EXISTS`, the retry will fail with `AlreadyExists` error, leaving you stuck.
- Similarly, undo scripts should use `DROP TABLE IF EXISTS` and `DROP INDEX IF EXISTS`.

> **Recommendation:** Treat every migration as potentially needing to be idempotent. Use `IF NOT EXISTS` for CREATE, `IF EXISTS` for DROP and ALTER operations.

### Repeatable Migrations

Repeatable migrations (`R__<description>.cql`) are re-applied whenever their content (checksum) changes. Be aware of the following:

- **Repeatable migrations must be fully idempotent.** They will run multiple times over the lifecycle of your project. Every statement inside must be safe to execute repeatedly.
- **They run after all versioned migrations.** On every `migrate` invocation, pending versioned migrations are applied first, then any repeatable migrations with changed checksums.
- **There is no ordering guarantee between repeatable migrations.** If you have multiple `R__*.cql` files, don't assume they run in any particular order. Each should be self-contained.
- **Checksums are validated only for versioned migrations.** Changes to repeatable migration files are expected — that's their purpose. The tool will re-apply them, not flag them as tampered.

Common use cases for repeatable migrations:
- Refreshing materialized views
- Recreating custom functions or aggregates
- Updating role permissions

> **Recommendation:** Keep repeatable migrations small and focused. If the content of a repeatable migration grows complex, consider splitting it into independent files.

## Development

```bash
# Run tests
make test

# Run tests with coverage
make test-coverage

# Lint
make lint

# Build
make build

# Build Docker image
make docker-build
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
