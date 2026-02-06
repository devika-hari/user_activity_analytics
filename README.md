# User Activity Analytics – ETL Pipeline

## Overview
This project implements an end-to-end ETL pipeline to ingest raw user activity logs, transform them into an analytics-ready data model, and load them into a PostgreSQL database for analysis.

The pipeline follows a **staging → marts** architecture and is built with clarity, data quality, and extensibility in mind. It is Airflow-ready, schema-driven, and supports idempotent data loads.

**Tech Stack:** Python, Apache Airflow, PostgreSQL, Docker

---

## What This Project Does

### 1. Extract (`extract.py`)
- Reads raw user activity data logs from a JSON file located at `DATA_PATH`.
- Performs basic validation and standardization:
  - Records with missing `user_id`, `action_type`, or `timestamp` are moved to `error_records.json` for further investigation.
    - Ideally, such records would remain in a raw layer; however, since this implementation starts at the staging layer, invalid records are excluded from staging.
  - Standardizes the `timestamp` field.
  - Derives `location` and `device` metadata.
  - Adds:
    - `load_ts` – load timestamp to identify each ingestion run
    - `is_processed` – processing flag (default: `FALSE`) for downstream mart loads
- Loads validated records into the PostgreSQL staging table `staging.stg_logs`.
- Moves the processed input file to `PROCESSED_PATH`.

---

### 2. Transform & Load (`transform_and_load.py`)
- Reads records from `staging.stg_logs` where `is_processed = FALSE`.
- Loads data into analytics marts using a star schema:
  - Surrogate primary keys are generated at the database level for all mart tables.
  - Foreign key relationships in the fact table are resolved through dimension lookups to ensure referential integrity.
  - Deduplication checks are applied before inserting into mart tables.
- Runs data quality checks after mart loads.
- Updates `is_processed` in staging after successful processing to ensure idempotency.

**Result:** Clean, structured, analytics-ready data in mart tables, with debug and error logs maintained for traceability.

---

### 3. Orchestration
- An Airflow DAG (`user_activity_dag.py`) orchestrates the pipeline with two tasks:
  - Extract
  - Transform & Load
- DAG name: `user_activity_pipeline`
- Scheduled to run **daily**, with the start date set to yesterday (configurable via DAG parameters).

---

### 4. Containerization
The solution is fully Dockerized and runs using four containers:

- **analytics-db** – PostgreSQL database (data only, no application code)
- **airflow-init** – One-time Airflow initialization (exits after setup)
- **airflow-webserver** – Airflow UI available at `http://localhost:8080`
- **airflow-scheduler** – Executes DAG tasks

---

## Data Model & Table Flow

### High-Level Flow
```
raw_logs.json
      ↓
 staging.stg_logs
      ↓
 ┌───────────────┬────────────────┐
 │               │                │
 dim_users     dim_actions        │
 │               │                │
 └───────┬───────┴───────┬────────┘
         ↓               ↓
        marts.fact_user_actions
```

---

## Database Design

### Database
- **Database Name:** `user_activity_analytics`
- **Schemas:**
  - `staging` – raw, cleaned data
  - `marts` – analytics-ready star schema

---

### Staging Table

**`staging.stg_logs`** – Stores raw user activity events

| Column        | Type      | Description                     |
|---------------|-----------|---------------------------------|
| user_id       | text      | Source user identifier           |
| timestamp     | timestamp | Event timestamp                  |
| action_type   | text      | User action (e.g. click, login)  |
| device        | text      | Device used                      |
| location      | text      | Location                         |
| is_processed  | boolean   | ETL processing flag              |
| load_ts       | timestamp | Load timestamp                   |

---

### Dimension Tables

**`marts.dim_users`**

| Column   | Type          | Description                |
|----------|---------------|----------------------------|
| user_key | integer (PK)  | Surrogate key              |
| user_id  | varchar       | Business key (unique)      |

**`marts.dim_actions`**

| Column      | Type          | Description                |
|-------------|---------------|----------------------------|
| action_key  | integer (PK)  | Surrogate key              |
| action_type | varchar       | Business key (unique)      |

---

### Fact Table

**`marts.fact_user_actions`**

| Column          | Type          | Description                     |
|-----------------|---------------|---------------------------------|
| fact_key        | integer (PK)  | Surrogate key                   |
| user_key        | integer (FK)  | References `dim_users`          |
| action_key      | integer (FK)  | References `dim_actions`        |
| event_timestamp | timestamp     | Event time                      |
| device          | varchar       | Device (degenerate dimension)   |
| location        | varchar       | Location (degenerate dimension) |

---

## Data Quality Rules
- Records with null `user_id`, `action_type`, or `timestamp` are excluded from staging and written to error records.
- No orphan facts:
  - Fact records are inserted only through joins with dimension tables.
  - Foreign key constraints enforce referential integrity.

---

## Configuration

All configuration is managed using environment variables via a `.env` file. A sample template (.env.template) is included in the repository.

> Note: Please update the database credentials in `schema.sql`, which contains all DDLs. OS-level environment variables will override values from `.env`.

---

## Prerequisites
- Docker & Docker Compose

### Python Packages (see `requirements.txt`)
- sqlalchemy
- psycopg2-binary
- pandas
- apache-airflow

---

## Setup Steps
1. Clone the repository
2. Ensure the `.env` file is fully populated (refer template).
3. Add the JSON file to data/raw/ in the project root and confirm that 'DATA_PATH' points to this file.
4. Build and start containers:
   ```bash
   docker-compose up --build
   ```
5. Verify all four containers are running (airflow-init exits after setup).
6. Open Airflow UI at `http://localhost:8080` .
7. Enable and run the `user_activity_pipeline` DAG manually if required.
   <img width="1426" height="556" alt="image" src="https://github.com/user-attachments/assets/d1c701d6-d2fb-4acc-a30d-40cbe5dd2faf" />

   

---

## Design Decisions & Trade-offs
- Records with null timestamps are excluded from staging:
  - Time is essential for fact-level analysis.
  - Keeping such records could introduce inconsistencies between staging and marts.
  - Invalid records are preserved separately for later review.
- Location and device are modeled as degenerate dimensions within the fact table to keep the schema simple.

---

## Production Notes
- Idempotent loads implemented using the `is_processed` flag.
- Schema-based modeling aligns with analytics best practices.

---

## Possible Enhancements
- KPI dashboards using Streamlit
- Additional security hardening
- Support for incremental file ingestion and partitioned tables

---

## Author
**Devika Hari**  
Data & Analytics Engineer

