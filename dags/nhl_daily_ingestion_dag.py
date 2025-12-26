from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator

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
from nhl_pipeline.config import get_settings

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
        # 1. Fetch Schedule (schedule/now)
        # This typically returns the current week's schedule (e.g. Mon-Sun)
        schedule_snapshot = fetch_schedule()
        schedule_uri = upload_snapshot_to_s3(schedule_snapshot, partition_dt=ts)
        print(f"Uploaded schedule snapshot: {schedule_uri}")

        # 2. Extract Games
        # We look back 7 days to ensure we catch any updates to recent games within the returned schedule window.
        # Note: 'schedule/now' returns a limited window. To look back further (e.g. start of season),
        # you must use the backfill DAG which iterates through specific dates.
        payload = schedule_snapshot.get("payload")
        
        # Parse execution date
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
            
        game_ids = extract_game_ids(
            payload,
            partition_dt=dt,
            lookback_days=7,
            only_final=True,
        )
        
        print(f"Found {len(game_ids)} FINAL games in the current schedule window")

        # 3. Ingest Games
        for game_id in game_ids:
            # Fetch and upload Boxscore
            box = fetch_game_boxscore(game_id)
            box_uri = upload_game_boxscore_snapshot_to_s3(
                box, game_id=game_id, partition_dt=ts
            )
            print(f"Uploaded boxscore: {game_id} -> {box_uri}")

            # Fetch and upload Play-by-Play
            pbp = fetch_game_play_by_play(game_id)
            pbp_uri = upload_game_pbp_snapshot_to_s3(
                pbp, game_id=game_id, partition_dt=ts
            )
            print(f"Uploaded pbp: {game_id} -> {pbp_uri}")

    task_ingest_daily = PythonOperator(
        task_id="ingest_daily",
        python_callable=ingest_daily,
        op_kwargs={"ts": "{{ ts }}"},
    )
