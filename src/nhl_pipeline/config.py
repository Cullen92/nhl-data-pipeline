from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    aws_region: str
    s3_bucket: str
    mwaa_bucket: str | None
    raw_prefix: str
    curated_prefix: str
    odds_api_key: str | None = None


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        # In MWAA or if file is missing, we return empty and rely on env vars
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping at {path}, got {type(data)}")
    return data


def _get_airflow_variable(key: str) -> str | None:
    """Try to retrieve a variable from Airflow's metadata database."""
    try:
        from airflow.models import Variable
        # default_var=None prevents KeyError if variable is missing
        return Variable.get(key, default_var=None)
    except ImportError:
        # Airflow not installed (e.g. local dev without airflow)
        return None
    except Exception as e:
        # Catch all exceptions (DB unreachable, connection issues, SQLAlchemy errors, etc.)
        # We log the exception details to aid debugging while gracefully degrading
        logger.warning(f"Unable to retrieve Airflow variable '{key}': {type(e).__name__}: {e}")
        return None


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

    # MWAA forces env vars to be lowercase (e.g. env.var.s3_bucket -> s3_bucket)
    # So we check both UPPERCASE (local/standard) and lowercase (MWAA)
    aws_region = (
        os.getenv("AWS_REGION")
        or os.getenv("aws_region")
        or _get_airflow_variable("AWS_REGION")
        or data.get("aws", {}).get("region")
    )
    s3_bucket = (
        os.getenv("S3_BUCKET")
        or os.getenv("s3_bucket")
        or _get_airflow_variable("S3_BUCKET")
        or data.get("s3", {}).get("bucket")
    )
    mwaa_bucket = (
        os.getenv("MWAA_BUCKET")
        or os.getenv("mwaa_bucket")
        or _get_airflow_variable("MWAA_BUCKET")
        or data.get("s3", {}).get("mwaa_bucket")
    )
    raw_prefix = os.getenv("S3_RAW_PREFIX") or data.get("paths", {}).get("raw_prefix", "raw/nhl")
    curated_prefix = os.getenv("S3_CURATED_PREFIX") or data.get("paths", {}).get("curated_prefix", "curated/nhl")
    odds_api_key = (
        os.getenv("ODDS_API_KEY")
        or os.getenv("odds_api_key")
        or _get_airflow_variable("ODDS_API_KEY")
        or data.get("odds_api", {}).get("api_key")
    )

    if not aws_region:
        raise ValueError("Missing required setting: aws.region. Check config/settings.yml or env vars.")
    if not s3_bucket:
        raise ValueError("Missing required setting: s3.bucket. Check config/settings.yml or env vars.")

    return Settings(
        aws_region=str(aws_region),
        s3_bucket=str(s3_bucket),
        mwaa_bucket=str(mwaa_bucket) if mwaa_bucket else None,
        raw_prefix=str(raw_prefix),
        curated_prefix=str(curated_prefix),
        odds_api_key=str(odds_api_key) if odds_api_key else None,
    )