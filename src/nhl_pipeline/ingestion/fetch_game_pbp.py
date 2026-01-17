"""
Fetch NHL game play-by-play data from the NHL API.

Retrieves detailed event-by-event data for games including shots,
goals, penalties, and other game events with coordinates and timestamps.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import boto3

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.api_utils import make_api_call
from nhl_pipeline.utils.datetime_utils import coerce_datetime, utc_now_iso
from nhl_pipeline.utils.paths import raw_game_pbp_key, utc_partition


def fetch_game_play_by_play(game_id: int | str, timeout_s: int = 30) -> dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    extracted_at = utc_now_iso()

    resp = make_api_call(url, timeout=timeout_s)
    payload = resp.json()

    return {
        "extracted_at": extracted_at,
        "source_url": url,
        "game_id": int(game_id),
        "payload": payload,
    }


def upload_game_pbp_snapshot_to_s3(
    snapshot: dict[str, Any],
    game_id: int | str,
    partition_dt: datetime | str | None = None,
) -> str:
    settings = get_settings()

    part = utc_partition(coerce_datetime(partition_dt))
    key = raw_game_pbp_key(part.date, part.hour, game_id=game_id)

    body = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")

    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    return f"s3://{settings.s3_bucket}/{key}"
