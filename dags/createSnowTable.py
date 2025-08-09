from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN_ID = "snowflake_conn"  # your Snowflake connection in Airflow

CREATE_TABLE_SQL = """
CREATE OR REPLACE TABLE airflowTestStage (
    id INT,
    name STRING,
    created_at TIMESTAMP
)
"""

INSERT_DUMMY_SQL = """
INSERT INTO airflowTestStage (id, name, created_at) VALUES
  (1, 'Alice', CURRENT_TIMESTAMP),
  (2, 'Bob', CURRENT_TIMESTAMP),
  (3, 'Charlie', CURRENT_TIMESTAMP);
"""

with DAG(
    dag_id="snowflake_create_and_insert_test",
    start_date=datetime(2025, 8, 1),
    schedule=None,   # manual trigger
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=CREATE_TABLE_SQL,
    )

    insert_dummy = SQLExecuteQueryOperator(
        task_id="insert_dummy",
        conn_id=CONN_ID,
        sql=INSERT_DUMMY_SQL,
    )

    create_table >> insert_dummy
