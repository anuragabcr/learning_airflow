import pandas as pd
import io
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

GCS_BUCKET_NAME = "learning-0101-airflow"
GCS_SILVER_FILE_PATH = "silver/flights_20251231T000000.csv"
GCS_FILE_PATH = "gold/flights_country_agg_{}.csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def transform_silver_to_gold(execution_date, **kwargs):
    logging.info(f"Downloading silver data from {GCS_SILVER_FILE_PATH}")

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=GCS_SILVER_FILE_PATH)

    silver_df = pd.read_csv(io.StringIO(file.decode('utf-8')))

    logging.info("Aggregating data to gold schema")

    gold_df = silver_df.groupby('origin_country').agg(
        total_flights=pd.NamedAgg(column='icao24', aggfunc='count'),
        avg_velocity=pd.NamedAgg(column='velocity', aggfunc='mean'),
        avg_baro_altitude=pd.NamedAgg(column='baro_altitude', aggfunc='mean'),
        on_ground_counts=pd.NamedAgg(column='on_ground', aggfunc='sum')
    ).reset_index()

    gold_df_csv = gold_df.to_csv(index=False)

    ts = execution_date.strftime("%Y%m%dT%H%M%S")
    dest_path = GCS_FILE_PATH.format(ts)

    logging.info(f"Uploading aggregated data to {dest_path}")

    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=dest_path,
        data=gold_df_csv,
        mime_type="text/csv",
    )

    logging.info("Upload complete.")

    return dest_path

with DAG(
    "transform_silver_data",
    default_args=default_args,
    description="Fetch flight data from silver layer, aggregate it, and upload to gold layer in GCS",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=["flight_ops", "gcs", "api"],
) as dag:
    transform_task = PythonOperator(
        task_id="transform_silver_to_gold",
        python_callable=transform_silver_to_gold,
        provide_context=True,
    )
    transform_task