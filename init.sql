-- Database initialization script
-- This runs when PostgreSQL container starts for the first time

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE data_warehouse TO postgres;

-- Create initial indexes (these will be created after tables are created by SQLAlchemy)
-- Examples of indexes that would be useful:
-- CREATE INDEX IF NOT EXISTS idx_dim_users_email ON dim_users(email);
-- CREATE INDEX IF NOT EXISTS idx_fact_sales_user_id ON fact_sales(user_id);
-- CREATE INDEX IF NOT EXISTS idx_fact_sales_date_id ON fact_sales(date_id);