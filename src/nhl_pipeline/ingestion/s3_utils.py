"""
Utilities for uploading data to AWS S3.

Provides functions for uploading JSON data to S3 with proper error handling
and checking if S3 objects already exist to avoid duplicate uploads.

Performance Note:
    When calling these functions in loops, pass a reused boto3 S3 client via the
    s3_client parameter to avoid creating a new client on every call. Example:

        s3 = boto3.client("s3", region_name=region)
        for key in keys:
            exists = s3_key_exists(bucket=bucket, key=key, s3_client=s3)
"""

from __future__ import annotations

import json
from typing import Any, Optional

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from nhl_pipeline.config import get_settings


def s3_key_exists(*, bucket: str, key: str, s3_client: Optional[BaseClient] = None) -> bool:
    """
    Check if an S3 key exists.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        s3_client: Optional reused boto3 S3 client for performance.
                   If None, a new client will be created.

    Returns:
        True if key exists, False otherwise

    Raises:
        PermissionError: If access is forbidden (403)
        ClientError: For other S3 errors
    """
    s3 = s3_client or boto3.client("s3", region_name=get_settings().aws_region)
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


def put_json_to_s3(*, bucket: str, key: str, payload: Any, s3_client: Optional[BaseClient] = None) -> str:
    """
    Upload JSON payload to S3.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        payload: JSON-serializable data to upload
        s3_client: Optional reused boto3 S3 client for performance.
                   If None, a new client will be created.

    Returns:
        S3 URI of uploaded object (s3://bucket/key)
    """
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s3 = s3_client or boto3.client("s3", region_name=get_settings().aws_region)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{bucket}/{key}"
