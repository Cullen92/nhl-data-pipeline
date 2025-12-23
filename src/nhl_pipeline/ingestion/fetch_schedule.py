from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import boto3
import requests

from nhl_pipeline.config import get_settings
from nhl_pipeline.utils.paths import raw_schedule_key, utc_partition

NHL_API_URL = "https://api-web.nhle.com/v1/schedule/now"


def fetch_schedule(url: str = NHL_API_URL, timeout_s: int = 30) -> dict[str, Any]:
    extracted_at = datetime.now(timezone.utc).isoformat()

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    payload = resp.json()

    # Raw wrapper: preserves lineage + makes snapshots auditable
    return {
        "extracted_at": extracted_at,
        "source_url": url,
        "payload": payload,
    }


def upload_snapshot_to_s3(snapshot: dict[str, Any]) -> str:
    settings = get_settings()

    part = utc_partition()
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


if __name__ == "__main__":
    snapshot = fetch_schedule()
    uri = upload_snapshot_to_s3(snapshot)
    print(f"Wrote raw schedule snapshot to: {uri}")