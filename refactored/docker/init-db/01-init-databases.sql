-- CDC Audit System Database Initialization
-- This script creates the source and sink databases

-- Create source database (to be monitored for changes)
CREATE DATABASE source_db;
GRANT ALL PRIVILEGES ON DATABASE source_db TO admin;

-- Create sink database (to store audit logs)
CREATE DATABASE sink_db;
GRANT ALL PRIVILEGES ON DATABASE sink_db TO admin;

-- Connect to source database and set up schema
\c source_db;

-- Create people table
CREATE TABLE IF NOT EXISTS people (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone_number TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create roles table
CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_people_email ON people(email);
CREATE INDEX IF NOT EXISTS idx_people_created_at ON people(created_at);
CREATE INDEX IF NOT EXISTS idx_roles_name ON roles(name);

-- Set replica identity for CDC
ALTER TABLE people REPLICA IDENTITY FULL;
ALTER TABLE roles REPLICA IDENTITY FULL;

-- Create publication for Debezium
DROP PUBLICATION IF EXISTS debezium_publication;
CREATE PUBLICATION debezium_publication FOR TABLE people, roles;

-- Insert sample data
INSERT INTO people (name, email, phone_number) VALUES
    ('John Doe', 'john.doe@example.com', '+1-555-0101'),
    ('Jane Smith', 'jane.smith@example.com', '+1-555-0102'),
    ('Alice Johnson', 'alice.johnson@example.com', '+1-555-0103'),
    ('Bob Brown', 'bob.brown@example.com', '+1-555-0104'),
    ('Charlie Davis', 'charlie.davis@example.com', '+1-555-0105')
ON CONFLICT (email) DO NOTHING;

INSERT INTO roles (name, description) VALUES
    ('Developer', 'Software development role'),
    ('Manager', 'Team management role'),
    ('Analyst', 'Data analysis role'),
    ('Designer', 'UI/UX design role'),
    ('Administrator', 'System administration role')
ON CONFLICT (name) DO NOTHING;

-- Connect to sink database and create audit schema
\c sink_db;

-- Create audit logging table
CREATE TABLE IF NOT EXISTS audit_logging (
    id BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    change_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    old_data JSONB,
    new_data JSONB,
    change_user TEXT,
    session_id TEXT NOT NULL DEFAULT gen_random_uuid()::text,
    transaction_id TEXT,
    lsn TEXT,
    source_metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create Kafka offset tracking table
CREATE TABLE IF NOT EXISTS kafka_offsets (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset BIGINT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    consumer_group TEXT NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processing_status TEXT NOT NULL DEFAULT 'completed'
);

-- Create processing errors table
CREATE TABLE IF NOT EXISTS processing_errors (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset BIGINT NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_traceback TEXT,
    message_key TEXT,
    message_value TEXT,
    occurred_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    status TEXT NOT NULL DEFAULT 'failed' CHECK (status IN ('failed', 'resolved', 'ignored'))
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_audit_logging_source_table ON audit_logging(source_table);
CREATE INDEX IF NOT EXISTS idx_audit_logging_operation_type ON audit_logging(operation_type);
CREATE INDEX IF NOT EXISTS idx_audit_logging_change_timestamp ON audit_logging(change_timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_logging_composite ON audit_logging(source_table, operation_type, change_timestamp);

CREATE INDEX IF NOT EXISTS idx_kafka_offsets_topic_partition ON kafka_offsets(topic, partition);
CREATE INDEX IF NOT EXISTS idx_kafka_offsets_consumer_group ON kafka_offsets(consumer_group);
CREATE INDEX IF NOT EXISTS idx_kafka_offsets_timestamp ON kafka_offsets(timestamp);

CREATE INDEX IF NOT EXISTS idx_processing_errors_status ON processing_errors(status);
CREATE INDEX IF NOT EXISTS idx_processing_errors_occurred_at ON processing_errors(occurred_at);

-- Create unique constraint for Kafka offsets
ALTER TABLE kafka_offsets DROP CONSTRAINT IF EXISTS uq_kafka_offset_topic_partition_group;
ALTER TABLE kafka_offsets ADD CONSTRAINT uq_kafka_offset_topic_partition_group
    UNIQUE (topic, partition, consumer_group);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;

-- Enable pg_stat_statements for performance monitoring (if available)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN
        -- Extension is available, ensure it's configured
        PERFORM pg_stat_statements_reset();
    END IF;
EXCEPTION
    WHEN undefined_function THEN
        -- pg_stat_statements not available, skip
        NULL;
END $$;