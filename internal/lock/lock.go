package lock

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/scylla-migrate/scylla-migrate/internal/driver"
)

const MigrationLockID = "migration_lock"

type Lock struct {
	ID        string
	LockedBy  string
	LockedAt  time.Time
	ExpiresAt time.Time
}

type LockManager struct {
	session  *driver.Session
	keyspace string
	lockID   string
	owner    string
	Logger   zerolog.Logger
}

func NewLockManager(session *driver.Session, keyspace string, logger zerolog.Logger) *LockManager {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	owner := fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])

	return &LockManager{
		session:  session,
		keyspace: keyspace,
		lockID:   MigrationLockID,
		owner:    owner,
		Logger:   logger,
	}
}

func (lm *LockManager) Acquire(timeout time.Duration) error {
	lm.Logger.Debug().
		Str("owner", lm.owner).
		Dur("timeout", timeout).
		Msg("Attempting to acquire migration lock")

	deadline := time.Now().Add(timeout)
	ttl := int(timeout.Seconds()) + 60 // extra buffer for TTL
	backoff := 1 * time.Second

	for time.Now().Before(deadline) {
		query := fmt.Sprintf(
			`INSERT INTO %s.schema_lock (lock_id, locked_by, locked_at, expires_at)
			 VALUES (?, ?, ?, ?)
			 IF NOT EXISTS
			 USING TTL %d`,
			lm.keyspace, ttl,
		)

		applied, err := lm.executeLWT(query, lm.lockID, lm.owner, time.Now(), time.Now().Add(timeout))
		if err != nil {
			return fmt.Errorf("failed to execute lock query: %w", err)
		}

		if applied {
			lm.Logger.Info().Str("owner", lm.owner).Msg("Migration lock acquired")
			return nil
		}

		// Lock is held by someone else — check if expired
		lock, err := lm.GetCurrentLock()
		if err != nil {
			if !errors.Is(err, gocql.ErrNotFound) {
				lm.Logger.Warn().Err(err).Msg("Failed to check current lock, retrying")
			}
			// Lock row doesn't exist or error — retry acquire
		} else if time.Now().After(lock.ExpiresAt) {
			lm.Logger.Warn().
				Str("held_by", lock.LockedBy).
				Time("expired_at", lock.ExpiresAt).
				Msg("Found expired lock, attempting to steal")

			if err := lm.forceRelease(); err != nil {
				lm.Logger.Warn().Err(err).Msg("Failed to release expired lock")
			}
			continue
		} else {
			lm.Logger.Debug().
				Str("held_by", lock.LockedBy).
				Time("expires_at", lock.ExpiresAt).
				Msg("Lock held by another process, waiting")
		}

		time.Sleep(backoff)
		if backoff < 10*time.Second {
			backoff = backoff * 2
		}
	}

	return fmt.Errorf("failed to acquire migration lock within %s — another migration may be in progress", timeout)
}

func (lm *LockManager) Release() error {
	lm.Logger.Debug().Str("owner", lm.owner).Msg("Releasing migration lock")

	query := fmt.Sprintf(
		`DELETE FROM %s.schema_lock WHERE lock_id = ? IF locked_by = ?`,
		lm.keyspace,
	)

	applied, err := lm.executeLWT(query, lm.lockID, lm.owner)
	if err != nil {
		// On release failure, force-delete as fallback
		lm.Logger.Warn().Err(err).Msg("LWT release failed, attempting force release")
		if ferr := lm.forceRelease(); ferr != nil {
			return fmt.Errorf("failed to release lock (LWT: %v, force: %v)", err, ferr)
		}
		lm.Logger.Info().Msg("Migration lock force-released")
		return nil
	}

	if !applied {
		lm.Logger.Warn().Msg("Lock was not released — it may have been stolen or expired")
		return nil
	}

	lm.Logger.Info().Msg("Migration lock released")
	return nil
}

func (lm *LockManager) GetCurrentLock() (*Lock, error) {
	query := fmt.Sprintf(
		`SELECT lock_id, locked_by, locked_at, expires_at FROM %s.schema_lock WHERE lock_id = ?`,
		lm.keyspace,
	)

	var lock Lock
	err := lm.session.Query(query, lm.lockID).Scan(
		&lock.ID, &lock.LockedBy, &lock.LockedAt, &lock.ExpiresAt,
	)
	if err != nil {
		return nil, err
	}

	return &lock, nil
}

func (lm *LockManager) forceRelease() error {
	query := fmt.Sprintf(
		`DELETE FROM %s.schema_lock WHERE lock_id = ?`,
		lm.keyspace,
	)
	return lm.session.Execute(query, lm.lockID)
}

func (lm *LockManager) executeLWT(query string, args ...interface{}) (bool, error) {
	q := lm.session.Query(query, args...)
	m := make(map[string]interface{})
	applied, err := q.MapScanCAS(m)
	if err != nil {
		return false, err
	}
	return applied, nil
}
