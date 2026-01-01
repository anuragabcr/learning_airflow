import json
import requests
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

API_URL = "https://opensky-network.org/api/states/all"
GCS_BUCKET_NAME = "learning-0101-airflow"
GCS_FILE_PATH = "bronze/flights_{}.json"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_upload_to_gcp(execution_date, **kwargs):
    logging.info(f"Fetching data from {API_URL} API")

    res = requests.get(API_URL)
    res.raise_for_status()
    data = res.json()

    ts = execution_date.strftime("%Y%m%dT%H%M%S")
    file_path = GCS_FILE_PATH.format(ts)

    logging.info(f"Uploading data to GCS bucket: {GCS_BUCKET_NAME}, path: {file_path}")

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=file_path,
        data=json.dumps(data),
        mime_type="application/json",
    )

    logging.info("Upload complete.")

with DAG(
    "api_to_gcs_loader",
    default_args=default_args,
    description="Fetch flight data from API and upload to GCS",
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
    extract_upload_task