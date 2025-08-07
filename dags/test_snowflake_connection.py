from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    start_date=datetime(2025, 8, 7),
    schedule="@once",  # Run manually only
    catchup=False,
    tags=["test", "snowflake"],
)
def test_snowflake_connection_dag():
    @task
    def test_connection():
        # Set up the connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

        # Get connection details and execute a test query
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Run a simple test query
            cursor.execute("SELECT CURRENT_VERSION();")
            result = cursor.fetchone()
            print(f"Snowflake version: {result[0]}")
            return f"Success! Snowflake version: {result[0]}"
        except Exception as e:
            print(f"Error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    test_connection()

dag = test_snowflake_connection_dag()
