-- Create users table

CREATE TABLE IF NOT EXISTS test_keyspace.users (
    id UUID PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMP
);
