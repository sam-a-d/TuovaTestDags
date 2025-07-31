from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
)
def hello_world_dag():
    @task
    def say_hello():
        print("Hello, Airflow is working!")

    say_hello()

dag = hello_world_dag()

