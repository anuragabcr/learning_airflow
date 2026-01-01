import pandas as pd
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

GCS_BUCKET_NAME = "learning-0101-airflow"
GCS_BRONZE_FILE_PATH = "bronze/flights_20260101T035159.json"
GCS_FILE_PATH = "silver/flights_{}.json"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def transform_bronze_to_silver(execution_date, **kwargs):
    logging.info(f"Downloading raw data from {GCS_BRONZE_FILE_PATH}")

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    raw_data = gcs_hook.download(bucket_name= GCS_BUCKET_NAME, object_name=GCS_BRONZE_FILE_PATH)
    df = json.loads(raw_data)

    logging.info("Transforming data to silver schema")

    if 'states' in df and df['states']:
        column_names = [
            'icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
            'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
            'true_track', 'vertical_rate', 'sensors', 'geo_altitude', 'squawk',
            'spi', 'position_source'
        ]
        
        states_df = pd.DataFrame(df['states'], columns=column_names)

        silver_df = states_df[['icao24','callsign','origin_country',
                            'longitude','latitude','baro_altitude',
                            'velocity','true_track']]
    else:
        silver_df = df
    
    silver_df_csv = silver_df.to_csv(index=False)

    ts = execution_date.strftime("%Y%m%dT%H%M%S")
    dest_path = GCS_FILE_PATH.format(ts)

    logging.info(f"Uploading transformed data to {dest_path}")

    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=dest_path,
        data=silver_df_csv,
        mime_type="text/csv",
    )

with DAG(
    "transform_bronze_data",
    default_args=default_args,
    description="Fetch flight data from Bronze layer, transform it, and upload to Silver layer in GCS",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=["flight_ops", "gcs", "api"],
) as dag:
    transform_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
        provide_context=True,
    )
    transform_task