from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    aws_region: str
    s3_bucket: str
    mwaa_bucket: str | None
    raw_prefix: str
    curated_prefix: str


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        # In MWAA or if file is missing, we return empty and rely on env vars
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping at {path}, got {type(data)}")
    return data


def get_settings() -> Settings:
    """
    Load settings from config/settings.yml, with environment variable overrides.

    Env var overrides:
      - AWS_REGION
      - S3_BUCKET
      - MWAA_BUCKET
      - S3_RAW_PREFIX
      - S3_CURATED_PREFIX
    """
    # Try to find config relative to CWD or project root
    data = _load_yaml(Path("config/settings.yml"))

    aws_region = os.getenv("AWS_REGION") or data.get("aws", {}).get("region")
    s3_bucket = os.getenv("S3_BUCKET") or data.get("s3", {}).get("bucket")
    mwaa_bucket = os.getenv("MWAA_BUCKET") or data.get("s3", {}).get("mwaa_bucket")
    raw_prefix = os.getenv("S3_RAW_PREFIX") or data.get("paths", {}).get("raw_prefix", "raw/nhl")
    curated_prefix = os.getenv("S3_CURATED_PREFIX") or data.get("paths", {}).get("curated_prefix", "curated/nhl")

    missing = [name for name, val in [("aws.region", aws_region), ("s3.bucket", s3_bucket)] if not val]
    if missing:
        raise ValueError(f"Missing required settings: {missing}. Check config/settings.yml or env vars.")

    return Settings(
        aws_region=str(aws_region),
        s3_bucket=str(s3_bucket),
        mwaa_bucket=str(mwaa_bucket) if mwaa_bucket else None,
        raw_prefix=str(raw_prefix),
        curated_prefix=str(curated_prefix),
    )