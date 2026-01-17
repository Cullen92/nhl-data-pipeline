"""
Fetch NHL skater statistics reports from the NHL Stats API.

Provides functions for fetching paginated skater statistics including:
- Summary stats (goals, assists, points)
- Time on ice reports 
- Power play statistics

!**** 
Currently not used in production pipeline. Reserved for future
detailed player statistics analysis.
!****

"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import boto3

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.api_utils import make_api_call
from nhl_pipeline.utils.datetime_utils import coerce_datetime, utc_now_iso
from nhl_pipeline.utils.paths import (
    raw_stats_skater_powerplay_key,
    raw_stats_skater_summary_key,
    raw_stats_skater_timeonice_key,
    utc_partition,
)

_STATS_BASE = "https://api.nhle.com/stats/rest/en"


def _stats_url(path: str, params: dict[str, Any]) -> str:
    return f"{_STATS_BASE}/{path}?{urlencode(params)}"

# TODO: look to shift toward aggregate stats for grabing season's worth rather than games
def fetch_stats_skater_summary(
    *,
    season_id: int,
    game_type_id: int,
    start: int = 0,
    limit: int = 1000,
    timeout_s: int = 30,
) -> dict[str, Any]:
    extracted_at = utc_now_iso()

    params = {
        "isAggregate": "false",
        "isGame": "true",
        "start": start,
        "limit": limit,
        "cayenneExp": f"seasonId={season_id} and gameTypeId={game_type_id}",
    }

    # Example URL: https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=true&start=0&limit=1000&cayenneExp=seasonId%3D20232024%20and%20gameTypeId%3D2
    url = _stats_url("skater/summary", params)

    resp = make_api_call(url, timeout=timeout_s)
    payload = resp.json()

    return {
        "extracted_at": extracted_at,
        "source_url": url,
        "season_id": season_id,
        "game_type_id": game_type_id,
        "start": start,
        "limit": limit,
        "payload": payload,
    }


def fetch_stats_skater_timeonice(
    *,
    season_id: int,
    game_type_id: int,
    start: int = 0,
    limit: int = 1000,
    timeout_s: int = 30,
) -> dict[str, Any]:
    extracted_at = utc_now_iso()

    params = {
        "isAggregate": "false",
        "isGame": "true",
        "start": start,
        "limit": limit,
        "cayenneExp": f"seasonId={season_id} and gameTypeId={game_type_id}",
    }
    url = _stats_url("skater/timeonice", params)

    resp = make_api_call(url, timeout=timeout_s)
    payload = resp.json()

    return {
        "extracted_at": extracted_at,
        "source_url": url,
        "season_id": season_id,
        "game_type_id": game_type_id,
        "start": start,
        "limit": limit,
        "payload": payload,
    }


def fetch_stats_skater_powerplay(
    *,
    season_id: int,
    game_type_id: int,
    start: int = 0,
    limit: int = 1000,
    timeout_s: int = 30,
) -> dict[str, Any]:
    extracted_at = utc_now_iso()

    params = {
        "isAggregate": "false",
        "isGame": "true",
        "start": start,
        "limit": limit,
        "cayenneExp": f"seasonId={season_id} and gameTypeId={game_type_id}",
    }
    url = _stats_url("skater/powerplay", params)

    resp = make_api_call(url, timeout=timeout_s)
    payload = resp.json()

    return {
        "extracted_at": extracted_at,
        "source_url": url,
        "season_id": season_id,
        "game_type_id": game_type_id,
        "start": start,
        "limit": limit,
        "payload": payload,
    }


def upload_stats_skater_summary_snapshot_to_s3(
    snapshot: dict[str, Any],
    *,
    season_id: int,
    game_type_id: int,
    start: int,
    partition_dt: datetime | str | None = None,
) -> str:
    settings = get_settings()
    part = utc_partition(coerce_datetime(partition_dt))
    key = raw_stats_skater_summary_key(part.date, part.hour, season_id, game_type_id, start=start)

    body = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{settings.s3_bucket}/{key}"


def upload_stats_skater_timeonice_snapshot_to_s3(
    snapshot: dict[str, Any],
    *,
    season_id: int,
    game_type_id: int,
    start: int,
    partition_dt: datetime | str | None = None,
) -> str:
    settings = get_settings()
    part = utc_partition(coerce_datetime(partition_dt))
    key = raw_stats_skater_timeonice_key(part.date, part.hour, season_id, game_type_id, start=start)

    body = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{settings.s3_bucket}/{key}"


def upload_stats_skater_powerplay_snapshot_to_s3(
    snapshot: dict[str, Any],
    *,
    season_id: int,
    game_type_id: int,
    start: int,
    partition_dt: datetime | str | None = None,
) -> str:
    settings = get_settings()
    part = utc_partition(coerce_datetime(partition_dt))
    key = raw_stats_skater_powerplay_key(part.date, part.hour, season_id, game_type_id, start=start)

    body = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{settings.s3_bucket}/{key}"
