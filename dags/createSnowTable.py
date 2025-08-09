from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Snowflake connection ID in Airflow
SNOWFLAKE_CONN_ID = "snowflake_conn"

# SQL to create a simple table
CREATE_TABLE_SQL = """
CREATE OR REPLACE TABLE airflowStageTable (
    id INT,
    name STRING,
    created_at TIMESTAMP
)
"""

# SQL to insert dummy data
INSERT_DUMMY_SQL = """
INSERT INTO airflowStageTable (id, name, created_at) VALUES
    (1, 'Alice', CURRENT_TIMESTAMP),
    (2, 'Bob', CURRENT_TIMESTAMP),
    (3, 'Charlie', CURRENT_TIMESTAMP)
"""

with DAG(
    dag_id="snowflake_create_and_insert_test",
    start_date=datetime(2025, 8, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    create_table = SnowflakeOperator(
        task_id="create_table_in_stage",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TABLE_SQL,
    )

    insert_dummy = SnowflakeOperator(
        task_id="insert_dummy_data",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=INSERT_DUMMY_SQL,
    )

    create_table >> insert_dummy
