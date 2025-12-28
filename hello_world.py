from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hello_world_composer',
    default_args=default_args,
    schedule_interval=None,  # run on demand
    catchup=False
) as dag:

    hello_task = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World from Airflow on Composer!"'
    )

    hello_task
