import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

# Ensure project root is on path so "etl" package is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# # Import the main functions from your etl_scripts
from etl_scripts.extract import run_extract
from etl_scripts.transform_and_load import run_transform_and_load

# Default arguments for the DAG
default_args = {
    'owner': 'data_eng',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'user_activity_pipeline',
    default_args=default_args,
    description='ETL pipeline for user activity logs',
    schedule='@daily',  # Runs every day
    tags=["etl", "user-actions"],
    catchup=False
) as dag:

    # Task 1: Extract from JSON and load to Staging
    extract_task = PythonOperator(
        task_id='extract_and_stage',
        python_callable=run_extract,
    )

    # Task 2: Transform from Staging to Marts
    transform_task = PythonOperator(
        task_id='stage_to_marts',
        python_callable=run_transform_and_load,
    )

    # Set dependencies (Extract -> Transform)
    extract_task >> transform_task