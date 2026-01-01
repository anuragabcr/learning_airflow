import pandas as pd
import io
import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook

GCS_BUCKET_NAME = "learning-0101-airflow"
GOLD_FILE_PATH_TEMPLATE = "gold/flight_country_agg_{ts}.csv"

def transform_silver_to_gold(**kwargs):
    ti = kwargs['ti']
    source_path = ti.xcom_pull(task_ids='transform_bronze_to_silver')

    if not source_path:
        raise ValueError("Source path not found in XComs.")
    
    logging.info(f"Downloading silver data from {source_path}")

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=source_path)

    # silver_df = pd.read_csv(io.StringIO(file.decode('utf-8')))
    silver_df = pd.read_csv(io.BytesIO(file))

    logging.info("Aggregating data to gold schema")
    gold_df = silver_df.groupby('origin_country').agg(
        total_flights=pd.NamedAgg(column='icao24', aggfunc='count'),
        avg_velocity=pd.NamedAgg(column='velocity', aggfunc='mean'),
        avg_baro_altitude=pd.NamedAgg(column='baro_altitude', aggfunc='mean'),
        on_ground_counts=pd.NamedAgg(column='on_ground', aggfunc='sum')
    ).reset_index()

    gold_df_csv = gold_df.to_csv(index=False)

    ts = kwargs['ts_nodash']
    dest_path = GOLD_FILE_PATH_TEMPLATE.format(ts=ts)

    logging.info(f"Uploading aggregated data to {dest_path}")

    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=dest_path,
        data=gold_df_csv,
        mime_type="text/csv",
    )

    logging.info("Upload complete.")

    return dest_path