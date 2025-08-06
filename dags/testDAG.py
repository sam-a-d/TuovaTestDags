from airflow.decorators import dag, task
from datetime import datetime

import tuovaEtl
import torch

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
)
def test_dag():
    @task
    def say_hello():
        print("The test is passed")

    say_hello()

dag = test_dag()

