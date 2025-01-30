CREATE DATABASE source_db;
CREATE DATABASE sink_db;

\c source_db;

-- Create people table
CREATE TABLE people (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create roles table
CREATE TABLE roles (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- set replica identity for both tables
ALTER TABLE people REPLICA IDENTITY FULL;
ALTER TABLE roles REPLICA IDENTITY FULL;

CREATE PUBLICATION debezium_publication FOR TABLE people, roles;

\c sink_db;

CREATE TABLE audit_logging (
    id SERIAL PRIMARY KEY,                  
    source_table TEXT NOT NULL,             
    operation_type TEXT NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    old_data JSONB,                         
    new_data JSONB,                         
    change_user TEXT                        
);

-- SELECT * FROM pg_replication_slots;