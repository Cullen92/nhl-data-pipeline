from __future__ import annotations

from datetime import datetime, timedelta
import os
import time

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
from nhl_pipeline.ingestion.gamecenter_selection import extract_game_ids
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
        
        # 1. Fetch Schedule (schedule/now)
        # This typically returns the current week's schedule (e.g. Mon-Sun)
        logger.info("Fetching schedule from NHL API...")
        schedule_snapshot = fetch_schedule()
        schedule_uri = upload_snapshot_to_s3(schedule_snapshot, partition_dt=ts)
        logger.info(f"✓ Uploaded schedule snapshot: {schedule_uri}")
        print(f"Uploaded schedule snapshot: {schedule_uri}")

        # 2. Extract Games
        # We look back 7 days to ensure we catch any updates to recent games within the returned schedule window.
        # Note: 'schedule/now' returns a limited window. To look back further (e.g. start of season),
        # you must use the backfill DAG which iterates through specific dates.
        logger.info("Extracting game IDs from schedule...")
        payload = schedule_snapshot.get("payload")
        
        # Parse execution date
        dt = parse_airflow_ts(ts)
            
        game_ids = extract_game_ids(
            payload,
            partition_dt=dt,
            lookback_days=7,
            only_final=True,
        )
        
        logger.info(f"✓ Found {len(game_ids)} FINAL games in the current schedule window (lookback: 7 days)")
        print(f"Found {len(game_ids)} FINAL games in the current schedule window")

        # 3. Ingest Games
        logger.info(f"Starting game ingestion for {len(game_ids)} games...")
        for idx, game_id in enumerate(game_ids, 1):
            logger.info(f"Processing game {idx}/{len(game_ids)}: {game_id}")
            
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
        
        logger.info(f"=== Completed NHL Daily Ingestion: {len(game_ids)} games processed ===")

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
                SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
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
                    $1, 
                    METADATA$FILENAME,
                    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e', 1)),
                    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'game_id=(\\d+)', 1, 1, 'e', 1))
                FROM @NHL.RAW_NHL.NHL_RAW_S3_STAGE/game_boxscore/
            )
            FILE_FORMAT=(TYPE=JSON)
            PATTERN='.*\\.json$'
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
                    $1, 
                    METADATA$FILENAME,
                    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e', 1)),
                    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'game_id=(\\d+)', 1, 1, 'e', 1))
                FROM @NHL.RAW_NHL.NHL_RAW_S3_STAGE/game_pbp/
            )
            FILE_FORMAT=(TYPE=JSON)
            PATTERN='.*\\.json$'
            ON_ERROR='CONTINUE';
        """,
        autocommit=True,
    )

    # Dependencies
    # S3 Ingestion must finish before we try to load any tables.
    # The 3 load tasks can run in parallel.
    task_ingest_daily >> [load_schedule, load_boxscores, load_pbp]
