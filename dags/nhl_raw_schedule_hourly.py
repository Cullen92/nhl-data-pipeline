from datetime import datetime, timedelta
from airflow.models import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.hooks.subprocess import SubprocessHook
from nhl_pipeline.ingestion.fetch_schedule import fetch_schedule

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

def run_script():
    hook = SubprocessHook()  # Create an instance of SubprocessHook
    command = ["python3", "-m", "src.ingestion.fetch_schedule"]
    hook.run_command(command)  # Use the `run_command` method to run an external script

task_fetch_schedule = PythonOperator(
    task_id="fetch_schedule",
    python_callable=fetch_schedule,
    dag=dag,
)