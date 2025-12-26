from __future__ import annotations

from datetime import datetime, timezone


def coerce_datetime(value: datetime | str | None) -> datetime | None:
    """Convert string or datetime to timezone-aware datetime.
    
    Handles Airflow's {{ ts }} format and ISO 8601 strings.
    If a string ends with 'Z', it's replaced with '+00:00'.
    If timezone is missing, UTC is assumed.
    
    Args:
        value: datetime, ISO string, or None
        
    Returns:
        Timezone-aware datetime or None
        
    Raises:
        TypeError: If value is not datetime, str, or None
    """
    if value is None or isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise TypeError(f"partition_dt must be datetime | str | None, got {type(value)}")

    # Airflow's {{ ts }} often renders like '2025-12-25T01:00:00+00:00' (or sometimes ends with 'Z').
    iso = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_airflow_ts(ts: str) -> datetime:
    """Parse Airflow {{ ts }} into a timezone-aware UTC datetime."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_utc_dt(value: str) -> datetime | None:
    """Parse UTC datetime string, handling 'Z' suffix and missing timezone.
    
    Args:
        value: ISO format datetime string
        
    Returns:
        Timezone-aware UTC datetime or None if parsing fails
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def utc_now_iso() -> str:
    """Return current UTC timestamp as ISO 8601 string.
    
    Useful for 'extracted_at' fields in API responses.
    """
    return datetime.now(timezone.utc).isoformat()
