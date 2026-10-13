-- ────────────────────────────────────────────────────────────────────
-- PostgreSQL setup for Smart Meter Streaming Analytics
-- Run this once:   psql -U postgres -f setup_db.sql
-- ────────────────────────────────────────────────────────────────────

-- Create the database (connect as a superuser)
SELECT 'CREATE DATABASE smartmeter'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'smartmeter')\gexec

-- Create application user (skip if already exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'meter_app') THEN
        CREATE ROLE meter_app WITH LOGIN PASSWORD 'meter_pass';
    END IF;
END
$$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE smartmeter TO meter_app;

-- Now connect to the smartmeter database
\c smartmeter

-- Grant schema-level privileges so meter_app can create tables
GRANT ALL ON SCHEMA public TO meter_app;

-- Individual readings with anomaly flag
CREATE TABLE IF NOT EXISTS meter_readings (
    reading_pk         BIGSERIAL PRIMARY KEY,
    reading_time       TIMESTAMP,
    global_active_power NUMERIC(10,3),
    rolling_avg_30     NUMERIC(10,3),
    is_anomaly         SMALLINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_readings_time ON meter_readings (reading_time);
CREATE INDEX IF NOT EXISTS idx_readings_pk   ON meter_readings (reading_pk DESC);

-- Singleton row with live aggregates (upserted on id=1)
CREATE TABLE IF NOT EXISTS meter_metrics (
    id              INT PRIMARY KEY,
    current_power   NUMERIC(10,3),
    rolling_avg     NUMERIC(10,3),
    peak_power      NUMERIC(10,3),
    total_kwh       NUMERIC(12,3),
    anomaly_count   INT
);

-- Grant table-level privileges and transfer ownership
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO meter_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO meter_app;
ALTER TABLE meter_readings OWNER TO meter_app;
ALTER TABLE meter_metrics OWNER TO meter_app;
ALTER SEQUENCE meter_readings_reading_pk_seq OWNER TO meter_app;
