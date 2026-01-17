"""
Fetch NHL schedule data from the NHL API.

Retrieves game schedules for specified date ranges, including game times,
teams, venues, and game states. Used to identify games for detailed analysis.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import boto3

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.api_utils import make_api_call
from nhl_pipeline.utils.datetime_utils import coerce_datetime, utc_now_iso
from nhl_pipeline.utils.paths import raw_schedule_key, utc_partition

NHL_API_URL = "https://api-web.nhle.com/v1/schedule/now"


def fetch_schedule(url: str = NHL_API_URL, timeout_s: int = 30) -> dict[str, Any]:
    extracted_at = utc_now_iso()

    resp = make_api_call(url, timeout=timeout_s)
    payload = resp.json()

    # Raw wrapper: preserves lineage + makes snapshots auditable
    return {
        "extracted_at": extracted_at,
        "source_url": url,
        "payload": payload,
    }


def upload_snapshot_to_s3(snapshot: dict[str, Any], partition_dt: datetime | str | None = None) -> str:
    settings = get_settings()

    # Example: 2024-04-27 15:00:00+00:00
    part = utc_partition(coerce_datetime(partition_dt))

    # Format: "raw/nhl/schedule/date={date}/hour={hour}/{snapshot_filename(date, hour)}"
    # Example: "raw/nhl/schedule/date=2024-04-27/hour=15/schedule_2024-04-27_15.json"
    key = raw_schedule_key(part.date, part.hour)

    body = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")

    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    return f"s3://{settings.s3_bucket}/{key}"

# Test run
if __name__ == "__main__":
    snapshot = fetch_schedule()
    uri = upload_snapshot_to_s3(snapshot)
    print(f"Wrote raw schedule snapshot to: {uri}")