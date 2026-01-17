from __future__ import annotations

from datetime import datetime, timedelta
import os
import time

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from nhl_pipeline.ingestion.fetch_game_boxscore import (
    fetch_game_boxscore,
    upload_game_boxscore_snapshot_to_s3,
)
from nhl_pipeline.ingestion.fetch_game_pbp import (
    fetch_game_play_by_play,
    upload_game_pbp_snapshot_to_s3,
)
from nhl_pipeline.ingestion.fetch_schedule import fetch_schedule, upload_snapshot_to_s3
from nhl_pipeline.ingestion.gamecenter_selection import extract_final_game_ids
from nhl_pipeline.utils.datetime_utils import parse_airflow_ts

logger = LoggingMixin().log

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nhl_daily_ingestion",
    default_args=default_args,
    description="Daily ingestion of NHL schedule and games (schedule/now)",
    schedule="@daily",
    catchup=False,
) as dag:

    def ingest_daily(ts: str):
        # Configure rate limiting (default 0.25 seconds between game fetches)
        sleep_s = float(os.getenv("NHL_DAILY_SLEEP_S", "0.25"))
        
        logger.info(f"=== Starting NHL Daily Ingestion for execution date: {ts} ===")
        
        # Parse execution date
        dt = parse_airflow_ts(ts)
        
        # 1. Fetch Schedule (schedule/now + previous week)
        # schedule/now returns the current week's schedule (Mon-Sun)
        # We also fetch the previous week to catch games that finished after the week rolled over
        logger.info("Fetching current week schedule from NHL API...")
        schedule_snapshot = fetch_schedule()
        schedule_uri = upload_snapshot_to_s3(schedule_snapshot, partition_dt=ts)
        logger.info(f"✓ Uploaded schedule snapshot: {schedule_uri}")
        print(f"Uploaded schedule snapshot: {schedule_uri}")
        
        # Fetch previous week's schedule to catch games that rolled off
        prev_week_date = (dt - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_week_url = f"https://api-web.nhle.com/v1/schedule/{prev_week_date}"
        logger.info(f"Fetching previous week schedule ({prev_week_date})...")
        prev_schedule_snapshot = fetch_schedule(url=prev_week_url)
        logger.info("✓ Fetched previous week schedule")
        prev_schedule_uri = upload_snapshot_to_s3(prev_schedule_snapshot, partition_dt=prev_week_date)
        logger.info(f"✓ Uploaded previous week schedule snapshot: {prev_schedule_uri}")
        print(f"Uploaded previous week schedule snapshot: {prev_schedule_uri}")

        # 2. Extract Games from both schedules
        # We look back 7 days to ensure we catch any updates to recent games.
        logger.info("Extracting game IDs from schedules...")
        payload = schedule_snapshot.get("payload")
        prev_payload = prev_schedule_snapshot.get("payload")
            
        game_ids = extract_final_game_ids(
            payload,
            partition_dt=dt,
            lookback_days=7,
            only_final=True,
        )
        
        # Also extract from previous week's schedule
        prev_game_ids = extract_final_game_ids(
            prev_payload,
            partition_dt=dt,
            lookback_days=7,
            only_final=True,
        )
        
        # Combine and dedupe
        all_game_ids = list(set(game_ids + prev_game_ids))
        all_game_ids.sort()
        
        logger.info(f"✓ Found {len(all_game_ids)} FINAL games (current week: {len(game_ids)}, prev week: {len(prev_game_ids)})")
        print(f"Found {len(all_game_ids)} FINAL games in combined schedule window")

        # 3. Ingest Games
        logger.info(f"Starting game ingestion for {len(all_game_ids)} games...")
        for idx, game_id in enumerate(all_game_ids, 1):
            logger.info(f"Processing game {idx}/{len(all_game_ids)}: {game_id}")
            
            # Fetch and upload Boxscore
            box = fetch_game_boxscore(game_id)
            box_uri = upload_game_boxscore_snapshot_to_s3(
                box, game_id=game_id, partition_dt=ts
            )
            logger.debug(f"  ✓ Boxscore uploaded: {box_uri}")
            print(f"Uploaded boxscore: {game_id} -> {box_uri}")

            # Fetch and upload Play-by-Play
            pbp = fetch_game_play_by_play(game_id)
            pbp_uri = upload_game_pbp_snapshot_to_s3(
                pbp, game_id=game_id, partition_dt=ts
            )
            logger.debug(f"  ✓ PBP uploaded: {pbp_uri}")
            print(f"Uploaded pbp: {game_id} -> {pbp_uri}")
            
            # Rate limiting: sleep between game fetches to avoid overwhelming the API
            if sleep_s > 0:
                time.sleep(sleep_s)
        
        logger.info(f"=== Completed NHL Daily Ingestion: {len(all_game_ids)} games processed ===")

    task_ingest_daily = PythonOperator(
        task_id="ingest_daily",
        python_callable=ingest_daily,
        op_kwargs={"ts": "{{ ts }}"},
    )

    # Snowflake Load Tasks
    # We use FORCE=FALSE (default) so we don't reload the same files if the DAG re-runs.
    # ON_ERROR='CONTINUE' allows the load to skip files with errors while continuing to load valid files.
    # This provides robustness for partial failures while maintaining idempotency.
    
    load_schedule = SQLExecuteQueryOperator(
        task_id="load_schedule_snowflake",
        conn_id="snowflake_default",
        sql="""
            COPY INTO NHL.RAW_NHL.SCHEDULE_SNAPSHOTS (payload, s3_key, ingest_ts)
            FROM (
                SELECT 
                    $1:payload,
                    METADATA$FILENAME,
                    CURRENT_TIMESTAMP()
                FROM @NHL.RAW_NHL.NHL_RAW_S3_STAGE/schedule/
            )
            FILE_FORMAT=(TYPE=JSON)
            PATTERN='.*\\.json$'
            ON_ERROR='CONTINUE';
        """,
        autocommit=True,
    )

    load_boxscores = SQLExecuteQueryOperator(
        task_id="load_boxscores_snowflake",
        conn_id="snowflake_default",
        sql="""
            COPY INTO NHL.RAW_NHL.GAME_BOXSCORE_SNAPSHOTS (payload, s3_key, partition_date, game_id)
            FROM (
                SELECT 
                    $1:payload,
                    METADATA$FILENAME,
                    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e')),
                    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'game_id=([0-9]+)', 1, 1, 'e'))
                FROM @NHL.RAW_NHL.NHL_RAW_S3_STAGE/game_boxscore/
            )
            FILE_FORMAT=(TYPE=JSON)
            PATTERN='.*\.json$'
            ON_ERROR='CONTINUE';
        """,
        autocommit=True,
    )

    load_pbp = SQLExecuteQueryOperator(
        task_id="load_pbp_snowflake",
        conn_id="snowflake_default",
        sql="""
            COPY INTO NHL.RAW_NHL.GAME_PBP_SNAPSHOTS (payload, s3_key, partition_date, game_id)
            FROM (
                SELECT 
                    $1:payload,
                    METADATA$FILENAME,
                    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e')),
                    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'game_id=([0-9]+)', 1, 1, 'e'))
                FROM @NHL.RAW_NHL.NHL_RAW_S3_STAGE/game_pbp/
            )
            FILE_FORMAT=(TYPE=JSON)
            PATTERN='.*\.json$'
            ON_ERROR='CONTINUE';
        """,
        autocommit=True,
    )

    # dbt Cloud - trigger job and wait for completion
    # Connection 'dbt_cloud_default' configured in Airflow with Account ID + API Token
    # Job ID from dbt Cloud after creating the job
    run_dbt_models = DbtCloudRunJobOperator(
        task_id="run_dbt_models",
        job_id="{{ var.value.DBT_CLOUD_JOB_ID }}",
        check_interval=30,  # Poll every 30 seconds
        timeout=600,  # 10 minute timeout
        wait_for_termination=True,
    )

    # Export to Google Sheets for Tableau Public
    # Runs the sheets_export module which reads from Snowflake and writes to Google Sheets
    def export_to_sheets():
        """Export dbt models to Google Sheets."""
        import os
        import tempfile
        import boto3
        from airflow.models import Variable
        
        # Download Google credentials from S3 (avoids Variable JSON escaping issues)
        s3 = boto3.client("s3")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            creds_obj = s3.get_object(
                Bucket="mwaa-bucket-nhl-cullenm-dev",
                Key="config/google-sheets-credentials.json"
            )
            creds_content = creds_obj["Body"].read().decode("utf-8")
            f.write(creds_content)
            creds_path = f.name
        
        os.environ["GOOGLE_SHEET_ID"] = Variable.get("GOOGLE_SHEET_ID").strip()
        os.environ["GOOGLE_SHEETS_CREDENTIALS"] = creds_path
        
        # Snowflake credentials
        os.environ["SNOWFLAKE_ACCOUNT"] = Variable.get("SNOWFLAKE_ACCOUNT").strip()
        os.environ["SNOWFLAKE_USER"] = Variable.get("SNOWFLAKE_USER").strip()
        os.environ["SNOWFLAKE_PASSWORD"] = Variable.get("SNOWFLAKE_PASSWORD").strip()
        os.environ["SNOWFLAKE_DATABASE"] = Variable.get("SNOWFLAKE_DATABASE").strip()
        os.environ["SNOWFLAKE_WAREHOUSE"] = Variable.get("SNOWFLAKE_WAREHOUSE").strip()
        os.environ["SNOWFLAKE_SCHEMA"] = Variable.get("SNOWFLAKE_SCHEMA").strip()
        os.environ["SNOWFLAKE_ROLE"] = Variable.get("SNOWFLAKE_ROLE").strip()
        
        from nhl_pipeline.export.sheets_export import main
        main()
        
        # Cleanup temp file
        os.unlink(creds_path)

    export_sheets = PythonOperator(
        task_id="export_to_google_sheets",
        python_callable=export_to_sheets,
    )

    # Dependencies
    # S3 Ingestion must finish before we try to load any tables.
    # The 3 load tasks can run in parallel, then dbt runs after all loads complete.
    # Finally, export to Google Sheets for Tableau.
    task_ingest_daily >> [load_schedule, load_boxscores, load_pbp]
    [load_schedule, load_boxscores, load_pbp] >> run_dbt_models
    run_dbt_models >> export_sheets
