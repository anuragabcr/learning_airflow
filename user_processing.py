from airflow.decorators import dag, task  # Correct import for 2.x
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.base import PokeReturnValue # Correct import for 2.x
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example']
)
def user_processing():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_user_table",
        conn_id="mysql_airflow_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                created_at TIMESTAMP
            );
        """
    )

    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        import requests
        res = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        if res.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=res.json())
        return PokeReturnValue(is_done=False)

    @task
    def extract_user(fake_users):
        return {
            "user_id": fake_users['id'],
            "first_name": fake_users['personalInfo']['firstName'],
            "last_name": fake_users['personalInfo']['lastName'],
            "email": fake_users['personalInfo']['email'],
            "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    @task
    def store_user(user_info):
        # MySQL logic: Use insert_rows instead of copy_expert
        hook = MySqlHook(mysql_conn_id='mysql_airflow_conn')
        
        # Format the data for insertion
        rows = [(
            user_info['user_id'], 
            user_info['first_name'], 
            user_info['last_name'], 
            user_info['email'], 
            user_info['created_at']
        )]
        
        hook.insert_rows(
            table='users',
            rows=rows,
            target_fields=['user_id', 'first_name', 'last_name', 'email', 'created_at']
        )

    # Define dependencies
    user_data = extract_user(is_api_available())
    create_table >> user_data >> store_user(user_data)

user_processing()