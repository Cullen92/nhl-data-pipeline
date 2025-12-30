from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

from airflow.models import DAG, Variable
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
from nhl_pipeline.ingestion.gamecenter_selection import extract_game_ids
from nhl_pipeline.ingestion.s3_utils import put_json_to_s3, s3_key_exists
from nhl_pipeline.utils.paths import (
    raw_game_boxscore_key,
    raw_game_pbp_key,
    raw_meta_backfill_gamecenter_success_key,
)
from nhl_pipeline.config import get_settings


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _parse_date_yyyy_mm_dd(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


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
    dag_id="nhl_backfill_gamecenter",
    default_args=default_args,
    description="Backfill NHL gamecenter boxscore + play-by-play snapshots for a date range (FINAL games only)",
    schedule=None,  # manual runs
) as dag:

    def backfill_gamecenter(partition_dt: str):
        settings = get_settings()

        # Configure the date range (UTC dates). Defaults to season start -> today.
        start_date = _env("NHL_BACKFILL_START_DATE", "2025-10-07")
        end_date = _env("NHL_BACKFILL_END_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

        start_dt = _parse_date_yyyy_mm_dd(start_date)
        end_dt = _parse_date_yyyy_mm_dd(end_date)
        if end_dt < start_dt:
            raise ValueError("NHL_BACKFILL_END_DATE must be >= NHL_BACKFILL_START_DATE")

        max_games_per_day = int(os.getenv("NHL_BACKFILL_MAX_GAMES_PER_DAY", "30"))
        # Check both Airflow Variable and environment variable for force flag
        force_env = os.getenv("NHL_BACKFILL_FORCE", "false").strip().lower() in {"1", "true", "yes"}
        force_var = Variable.get("NHL_BACKFILL_FORCE", default_var="false").strip().lower() in {"1", "true", "yes"}
        force = force_env or force_var
        skip_existing_game_files = (
            os.getenv("NHL_BACKFILL_SKIP_EXISTING_GAME_FILES", "true").strip().lower() in {"1", "true", "yes"}
        )
        sleep_s = float(os.getenv("NHL_BACKFILL_SLEEP_S", "0"))

        day = start_dt
        total_games = 0
        while day <= end_dt:
            day_str = day.strftime("%Y-%m-%d")

            success_key = raw_meta_backfill_gamecenter_success_key(day_str)
            if not force and s3_key_exists(bucket=settings.s3_bucket, key=success_key):
                print(f"{day_str}: _SUCCESS exists; skipping")
                day += timedelta(days=1)
                continue

            url = f"https://api-web.nhle.com/v1/schedule/{day_str}"

            schedule_snapshot = fetch_schedule(url=url)
            payload = schedule_snapshot.get("payload")

            game_ids = extract_game_ids(
                payload,
                partition_dt=day,
                lookback_days=0,
                only_final=True,
                max_games=max_games_per_day,
            )

            print(f"{day_str}: found {len(game_ids)} FINAL games")

            started_at = datetime.now(timezone.utc).isoformat()
            for game_id in game_ids:
                hour = "00"  # day is midnight UTC

                box_key = raw_game_boxscore_key(day_str, hour, game_id)
                pbp_key = raw_game_pbp_key(day_str, hour, game_id)

                if skip_existing_game_files and s3_key_exists(bucket=settings.s3_bucket, key=box_key):
                    print(f"Skip existing boxscore: game_id={game_id}")
                else:
                    box = fetch_game_boxscore(game_id)
                    box_uri = upload_game_boxscore_snapshot_to_s3(
                        box, game_id=game_id, partition_dt=day.isoformat()
                    )
                    print(f"Uploaded boxscore: game_id={game_id} -> {box_uri}")

                if skip_existing_game_files and s3_key_exists(bucket=settings.s3_bucket, key=pbp_key):
                    print(f"Skip existing play-by-play: game_id={game_id}")
                else:
                    pbp = fetch_game_play_by_play(game_id)
                    pbp_uri = upload_game_pbp_snapshot_to_s3(
                        pbp, game_id=game_id, partition_dt=day.isoformat()
                    )
                    print(f"Uploaded play-by-play: game_id={game_id} -> {pbp_uri}")

                total_games += 1

                if sleep_s > 0:
                    time.sleep(sleep_s)

            # Only write success marker if:
            # 1. We found and processed games, OR
            # 2. The date is in the past (genuinely no games scheduled)
            # This prevents marking today/future dates as complete when games aren't FINAL yet
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            if len(game_ids) > 0 or day < today:
                completed_at = datetime.now(timezone.utc).isoformat()
                marker = {
                    "pipeline": "nhl_backfill_gamecenter",
                    "date": day_str,
                    "games": len(game_ids),
                    "game_ids": game_ids,
                    "started_at": started_at,
                    "completed_at": completed_at,
                }
                marker_uri = put_json_to_s3(bucket=settings.s3_bucket, key=success_key, payload=marker)
                print(f"{day_str}: wrote _SUCCESS marker -> {marker_uri}")
            else:
                print(f"{day_str}: skipping _SUCCESS marker (no FINAL games found, date is today or future)")

            day += timedelta(days=1)

        print(f"Backfill complete. Total games ingested: {total_games}")

    task_backfill_gamecenter = PythonOperator(
        task_id="backfill_gamecenter",
        python_callable=backfill_gamecenter,
        op_kwargs={"partition_dt": "{{ ts }}"},
    )
