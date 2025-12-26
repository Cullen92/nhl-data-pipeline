from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Any, Callable

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator

from nhl_pipeline.ingestion.fetch_stats_skater_reports import (
    fetch_stats_skater_powerplay,
    fetch_stats_skater_summary,
    fetch_stats_skater_timeonice,
    upload_stats_skater_powerplay_snapshot_to_s3,
    upload_stats_skater_summary_snapshot_to_s3,
    upload_stats_skater_timeonice_snapshot_to_s3,
)


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def _payload_row_count(snapshot: dict[str, Any]) -> int:
    payload = snapshot.get("payload")
    if not isinstance(payload, dict):
        return 0
    data = payload.get("data")
    if not isinstance(data, list):
        return 0
    return len(data)


# DAG default arguments
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
    dag_id="nhl_raw_stats_skater_daily",
    default_args=default_args,
    description="Fetch NHL stats REST skater reports (summary/timeonice/powerplay) and write raw snapshots to S3 (daily)",
    schedule="@daily",
) as dag:

    def ingest_stats(partition_dt: str):
        # Defaults chosen to match your current focus (2025-26 regular season).
        season_id = _env_int("NHL_SEASON_ID", 20252026)
        game_type_id = _env_int("NHL_GAME_TYPE_ID", 2)
        start0 = _env_int("NHL_STATS_START", 0)
        limit = _env_int("NHL_STATS_LIMIT", 1000)
        max_pages = _env_int("NHL_STATS_MAX_PAGES", 50)
        sleep_s = float(os.getenv("NHL_STATS_SLEEP_S", "0.25"))

        if limit <= 0:
            raise ValueError("NHL_STATS_LIMIT must be > 0")
        if max_pages <= 0:
            raise ValueError("NHL_STATS_MAX_PAGES must be > 0")

        def paginate(
            *,
            label: str,
            fetch_fn: Callable[..., dict[str, Any]],
            upload_fn: Callable[..., str],
        ) -> None:
            start = start0
            total_rows = 0
            pages = 0

            while pages < max_pages:
                snap = fetch_fn(
                    season_id=season_id,
                    game_type_id=game_type_id,
                    start=start,
                    limit=limit,
                )

                uri = upload_fn(
                    snap,
                    season_id=season_id,
                    game_type_id=game_type_id,
                    start=start,
                    partition_dt=partition_dt,
                )

                rows = _payload_row_count(snap)
                total_rows += rows
                pages += 1

                print(f"Uploaded {label}: start={start} rows={rows} -> {uri}")

                # If we got fewer rows than requested, we've reached the end.
                if rows < limit:
                    break

                start += limit
                if sleep_s > 0:
                    time.sleep(sleep_s)

            print(f"Finished {label}: pages={pages} total_rows={total_rows}")

        # Pull the full result sets in bounded chunks.
        paginate(label="skater summary", fetch_fn=fetch_stats_skater_summary, upload_fn=upload_stats_skater_summary_snapshot_to_s3)
        paginate(label="skater timeonice", fetch_fn=fetch_stats_skater_timeonice, upload_fn=upload_stats_skater_timeonice_snapshot_to_s3)
        paginate(label="skater powerplay", fetch_fn=fetch_stats_skater_powerplay, upload_fn=upload_stats_skater_powerplay_snapshot_to_s3)

    task_ingest_stats = PythonOperator(
        task_id="ingest_stats",
        python_callable=ingest_stats,
        op_kwargs={"partition_dt": "{{ ts }}"},
    )
