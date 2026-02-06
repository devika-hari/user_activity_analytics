------------------------------------------------
--Description: DDLs for Database execution
------------------------------------------------

-- Create analytics database
CREATE DATABASE user_activity_analytics;
-- Create ETL user for analytics DB
CREATE USER etl_user WITH PASSWORD 'etl_password';
-- Grant privileges and set ownership
GRANT ALL PRIVILEGES ON DATABASE user_activity_analytics TO etl_user;
ALTER DATABASE USER_ACTIVITY_ANALYTICS OWNER TO etl_user;

-- Create Airflow metadata database and user
CREATE DATABASE airflow;
-- Create airflow_user for airflow-orchestration
CREATE USER airflow_user WITH PASSWORD 'airflow_password';
-- Grant privileges and set ownership (also for public schema)
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;
\c airflow
ALTER SCHEMA public OWNER TO airflow_user;

----------------------------------------------------------------
--Switch to analytics database so schemas and tables are created here
\c user_activity_analytics

-- -- Database: user_activity_analytics

------------------------------------------------
-- Schemas
------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION etl_user;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION etl_user;

------------------------------------------------
-- Tables
------------------------------------------------
-- Table: staging.stg_logs
-- DROP TABLE IF EXISTS staging.stg_logs;
CREATE TABLE IF NOT EXISTS staging.stg_logs
(
    user_id text NOT NULL,
    "timestamp" timestamp without time zone,
    action_type text NOT NULL,
    device text ,
    location text,
    is_processed boolean DEFAULT false,
    load_ts timestamp without time zone NOT NULL
);

-- Table: marts.dim_actions
-- DROP TABLE IF EXISTS marts.dim_actions;
CREATE TABLE IF NOT EXISTS marts.dim_actions
(
    action_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action_type character varying(50) NOT NULL UNIQUE
);

-- Table: marts.dim_users
-- DROP TABLE IF EXISTS marts.dim_users;
CREATE TABLE IF NOT EXISTS marts.dim_users
(
    user_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id character varying(50) NOT NULL UNIQUE
);

-- Table: marts.fact_user_actions
-- DROP TABLE IF EXISTS marts.fact_user_actions;
CREATE TABLE IF NOT EXISTS marts.fact_user_actions
(
    fact_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_key integer NOT NULL,
    action_key integer NOT NULL,
    event_timestamp timestamp without time zone  NOT NULL,
    device character varying(100),
    location character varying(100) ,
    CONSTRAINT fact_user_actions_action_key_fkey FOREIGN KEY (action_key)
        REFERENCES marts.dim_actions (action_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fact_user_actions_user_key_fkey FOREIGN KEY (user_key)
        REFERENCES marts.dim_users (user_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

------------------------------------------------
-- Granting Privileges and ownership
------------------------------------------------
--Grant all privileges on all tables in staging and marts schemas to etl_user
ALTER TABLE staging.stg_logs OWNER TO etl_user;
ALTER TABLE marts.dim_actions OWNER TO etl_user;
ALTER TABLE marts.dim_users OWNER TO etl_user;
ALTER TABLE marts.fact_user_actions OWNER TO etl_user;
-- Grant usage and create privileges on schemas
GRANT USAGE, CREATE ON SCHEMA staging TO etl_user;
GRANT USAGE, CREATE ON SCHEMA marts TO etl_user;

-- Grant all privileges on all tables (explicit for safety)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO etl_user;

-- Grant privileges on sequences (for IDENTITY columns)
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA marts TO etl_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON SEQUENCES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON SEQUENCES TO etl_user;
