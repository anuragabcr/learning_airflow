import pandas as pd
import json
import logging
import airflow.providers.google.cloud.hooks.gcs as GCSHook

GCS_BUCKET_NAME = "learning-0101-airflow"
GCS_FILE_PATH_TEMPLATE = "silver/flights_{ts}.json" 

def transform_bronze_to_silver(**kwargs):
    ki = kwargs['ti']
    source_path = ki.xcom_pull(task_ids='fetch_and_upload_to_gcp')

    if not source_path:
        raise ValueError("Source path not found in XComs.")
    
    logging.info(f"Downloading raw data from {source_path}")

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    raw_data = gcs_hook.download(bucket_name= GCS_BUCKET_NAME, object_name=source_path)
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

    ts = kwargs['ts_nodash']
    dest_path = GCS_FILE_PATH_TEMPLATE.format(ts=ts)

    logging.info(f"Uploading transformed data to {dest_path}")

    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=dest_path,
        data=silver_df_csv,
        mime_type="text/csv",
    )

    logging.info("Upload complete.")

    return dest_path
