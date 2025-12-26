from __future__ import annotations

from datetime import datetime, timedelta
import os

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
from nhl_pipeline.ingestion.fetch_schedule import fetch_schedule

from nhl_pipeline.ingestion.gamecenter_selection import (
    extract_game_ids,
    parse_airflow_ts,
    partition_date_from_ts,
)


# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 7),  # NHL opening night 2025
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nhl_raw_gamecenter_hourly",
    default_args=default_args,
    description="Fetch NHL gamecenter boxscore + play-by-play and write raw snapshots to S3 (hourly)",
    schedule="15 * * * *",  # hourly trigger (offset from schedule DAG)
) as dag:

    def ingest_gamecenter(partition_dt: str):
        schedule_snapshot = fetch_schedule()
        payload = schedule_snapshot.get("payload")

        dt = parse_airflow_ts(partition_dt)
        partition_date = partition_date_from_ts(partition_dt)

        max_games = int(os.getenv("NHL_GAMECENTER_MAX_GAMES", "30"))
        lookback_days = int(os.getenv("NHL_GAMECENTER_LOOKBACK_DAYS", "3"))
        only_final = os.getenv("NHL_GAMECENTER_ONLY_FINAL", "true").strip().lower() in {"1", "true", "yes"}

        game_ids = extract_game_ids(
            payload,
            partition_dt=dt,
            lookback_days=lookback_days,
            only_final=only_final,
            max_games=max_games,
        )
        if not game_ids:
            print(f"No game IDs found for partition_date={partition_date}; nothing to ingest.")
            return

        print(f"Found {len(game_ids)} game_ids for partition_date={partition_date}")

        for game_id in game_ids:
            box = fetch_game_boxscore(game_id)
            box_uri = upload_game_boxscore_snapshot_to_s3(box, game_id=game_id, partition_dt=partition_dt)
            print(f"Uploaded game boxscore: game_id={game_id} -> {box_uri}")

            pbp = fetch_game_play_by_play(game_id)
            pbp_uri = upload_game_pbp_snapshot_to_s3(pbp, game_id=game_id, partition_dt=partition_dt)
            print(f"Uploaded game play-by-play: game_id={game_id} -> {pbp_uri}")

    task_ingest_gamecenter = PythonOperator(
        task_id="ingest_gamecenter",
        python_callable=ingest_gamecenter,
        op_kwargs={"partition_dt": "{{ ts }}"},
    )
