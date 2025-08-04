from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.empty import EmptyOperator  # Updated import

with DAG(
    dag_id="gcs_file_presence_detector",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["gcs", "sensor"],
) as dag:
    # This sensor waits for a specific object (file) to appear in the GCS bucket.
    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_my_erp_file",
        bucket="airflow-bucket-tuova",  # Your bucket name
        object="data/erp/2025/august/file.txt",  # The specific file path within the bucket
        google_cloud_conn_id="google_cloud_default",
        mode="reschedule",  # Recommended for long waits with triggerer enabled
        poke_interval=10,  # Check every 10 seconds
        timeout=60 * 60,  # Timeout after 1 hour
        deferrable=True,  # Explicitly mark as deferrable
    )

    # This task will run only after 'wait_for_file' successfully detects the file.
    file_detected_task = EmptyOperator(  # Changed to EmptyOperator
        task_id="erp_file_has_been_detected",
    )

    wait_for_file >> file_detected_task
