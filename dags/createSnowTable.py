from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlOperator

SNOWFLAKE_CONN_ID = "snowflake_conn"  # change if yours is different

CREATE_TABLE_SQL = """
CREATE OR REPLACE TABLE my_stage_table (
    id INT,
    name STRING,
    created_at TIMESTAMP
)
"""

INSERT_DUMMY_SQL = """
INSERT INTO my_stage_table (id, name, created_at) VALUES
  (1, 'Alice', CURRENT_TIMESTAMP),
  (2, 'Bob',   CURRENT_TIMESTAMP),
  (3, 'Charlie', CURRENT_TIMESTAMP);
"""

with DAG(
    dag_id="snowflake_create_and_insert_test",
    start_date=datetime(2025, 8, 1),
    schedule=None,       # manual only
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    create_table = SnowflakeSqlOperator(
        task_id="create_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TABLE_SQL,
    )

    insert_dummy = SnowflakeSqlOperator(
        task_id="insert_dummy",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=INSERT_DUMMY_SQL,
    )

    create_table >> insert_dummy
