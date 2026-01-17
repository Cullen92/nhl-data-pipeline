"""
Utilities for uploading data to AWS S3.

Provides functions for uploading JSON data to S3 with proper error handling
and checking if S3 objects already exist to avoid duplicate uploads.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from nhl_pipeline.config import get_settings


def s3_key_exists(*, bucket: str, key: str) -> bool:
    s3 = boto3.client("s3", region_name=get_settings().aws_region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "403":
            raise PermissionError(
                f"S3 HeadObject forbidden; cannot safely run missing-only backfill. "
                f"Grant s3:GetObject on arn:aws:s3:::{bucket}/* (or broader) for the MWAA execution role. "
                f"Failing key: s3://{bucket}/{key}"
            ) from e
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def put_json_to_s3(*, bucket: str, key: str, payload: Any) -> str:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s3 = boto3.client("s3", region_name=get_settings().aws_region)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{bucket}/{key}"
