-- Add secondary index on email

CREATE INDEX IF NOT EXISTS users_email_idx ON test_keyspace.users (email);
