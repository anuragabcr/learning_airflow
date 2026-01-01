from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.bronze_ingest import fetch_and_upload_to_gcp
from scripts.silver_transform import transform_bronze_to_silver
from scripts.gold_aggregate import transform_silver_to_gold

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
    bronze_task = PythonOperator(
        task_id="fetch_and_upload_to_gcp",
        python_callable=fetch_and_upload_to_gcp,
        provide_context=True,
    )
    silver_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
        provide_context=True,
    )
    gold_task = PythonOperator(
        task_id="transform_silver_to_gold",
        python_callable=transform_silver_to_gold,
        provide_context=True,
    )
    bronze_task >> silver_task >> gold_task