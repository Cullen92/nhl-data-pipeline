from datetime import datetime, timedelta
from airflow.models import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from nhl_pipeline.ingestion.fetch_schedule import fetch_schedule, upload_snapshot_to_s3

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 7),  # backfill start point - NHL opening night 2025
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "nhl_raw_schedule_hourly",
    default_args=default_args,
    description="Fetch NHL schedule and write to S3 (hourly)",
    schedule="0 * * * *",  # hourly trigger
)

def ingest_schedule():
    """Fetch schedule and upload to S3."""
    snapshot = fetch_schedule()
    uri = upload_snapshot_to_s3(snapshot)
    print(f"Successfully uploaded schedule to {uri}")

task_fetch_schedule = PythonOperator(
    task_id="fetch_schedule",
    python_callable=ingest_schedule,
    dag=dag,
)