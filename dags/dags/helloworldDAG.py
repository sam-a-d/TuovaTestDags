from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello, Airflow is working!")

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',  # Run only once
    catchup=False,              # Do not backfill missed runs
    tags=['example'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello,
    )

    hello_task  # This defines the task in the DAG
