-- Create customers table
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    stock INT NOT NULL CHECK (stock >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE PUBLICATION debezium_publication FOR TABLE customers, products;

SELECT * FROM pg_replication_slots;


CREATE TABLE audit_logging (
    id SERIAL PRIMARY KEY,                  
    source_table TEXT NOT NULL,             
    operation_type TEXT NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    old_data JSONB,                         
    new_data JSONB,                         
    change_user TEXT                        
);
