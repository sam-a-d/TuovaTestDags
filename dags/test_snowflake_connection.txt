from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator

def test_snowflake_connection():
    # Set up the connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Get connection details and execute a test query
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Run a simple test query (e.g., SELECT CURRENT_VERSION())
        cursor.execute("SELECT CURRENT_VERSION();")
        result = cursor.fetchone()  # Fetch the result
        print(f"Snowflake version: {result[0]}")  # This will print the Snowflake version
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()

# Define the DAG
with DAG(
    dag_id='test_snowflake_connection',
    default_args={'owner': 'airflow', 'start_date': days_ago(1)},
    schedule_interval=None,  # Run this only manually
    catchup=False,
) as dag:

    test_connection = PythonOperator(
        task_id='test_snowflake_connection',
        python_callable=test_snowflake_connection
    )

