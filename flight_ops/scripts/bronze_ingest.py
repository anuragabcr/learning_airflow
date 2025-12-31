import json
import requests
import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook

API_URL = "https://opensky-network.org/api/states/all"
GCS_BUCKET_NAME = "learning-3112-airflow"
GCS_FILE_PATH_TEMPLATE = "bronze/{date}/flights_{ts}.json" 

def fetch_and_upload_to_gcp(**kwargs):
    """
    Ingests raw data from API to GCS Bronze layer.
    """
    # 1. Extract context variables for file naming
    # 'ts_nodash' is the execution timestamp without formatting (e.g., 20230101T000000)
    # 'ds' is the date string (YYYY-MM-DD)
    ts = kwargs['ts_nodash'] 
    date_str = kwargs['ds']

    logging.info(f"Fetching data from {API_URL} API for execution date: {date_str}")

    # 2. API Request
    res = requests.get(API_URL)
    res.raise_for_status()
    data = res.json()

    # 3. Format Destination Path
    # saves as: bronze/2023-10-25/flights_20231025T120000.json
    file_path = GCS_FILE_PATH_TEMPLATE.format(date=date_str, ts=ts)

    logging.info(f"Uploading data to GCS bucket: {GCS_BUCKET_NAME}, path: {file_path}")

    # 4. Upload
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=file_path,
        data=json.dumps(data),
        mime_type="application/json",
    )

    logging.info("Upload complete.")

    # 5. Return path for XCom (The next task will grab this path to know what to process)
    return file_path