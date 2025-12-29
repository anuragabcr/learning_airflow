from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context
from datetime import datetime
import requests

USER_API_DATA = Dataset("https://randomuser.me/api")
USER_LOCATION = Dataset("user_location")
USER_LOGIN = Dataset("user_login")

# --- DAG 1: Produces the User Data ---
with DAG(
    dag_id="fetch_user_asset",
    start_date=datetime(2024, 1, 1),
    schedule="0 12 * * *",
    catchup=False,
) as dag1:

    @task(outlets=[USER_API_DATA])
    def fetch_user():
        res = requests.get("https://randomuser.me/api")
        res.raise_for_status()
        return res.json()

    fetch_user()

# --- DAG 2: Consumes User Data and Produces Location/Login ---
with DAG(
    dag_id="process_user_info",
    start_date=datetime(2024, 1, 1),
    schedule=[USER_API_DATA], # Triggered when USER_API_DATA is updated
    catchup=False,
) as dag2:

    @task(outlets=[USER_LOCATION, USER_LOGIN])
    def user_info():
        context = get_current_context()
        ti = context['ti']
        
        # Cross-DAG XCom pull (Note: dag_id must match the producer DAG)
        user_data = ti.xcom_pull(
            dag_id='fetch_user_asset',
            task_ids='fetch_user',
            include_prior_dates=True
        )
        
        if not user_data:
            raise ValueError("No user data found in XCom")

        location = user_data['results'][0]['location']
        login = user_data['results'][0]['login']
        
        # In Airflow 2, the task returning these values doesn't 
        # "automatically" assign them to specific datasets; 
        # the 'outlets' parameter handles the signaling.
        return [location, login]

    user_info()