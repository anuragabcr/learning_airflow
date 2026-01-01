from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from flight_ops.scripts.bronze_ingest import fetch_and_upload_to_gcp
from flight_ops.scripts.silver_transform import transform_bronze_to_silver

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "process_flight_data",
    default_args=default_args,
    description="Fetch flight data from API, process it, and upload to GCS",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=["flight_ops", "gcs", "api"],
) as dag:
    extract_upload_task = PythonOperator(
        task_id="fetch_and_upload_to_gcp",
        python_callable=fetch_and_upload_to_gcp,
        provide_context=True,
    )
    transform_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
        provide_context=True,
    )
    extract_upload_task >> transform_task