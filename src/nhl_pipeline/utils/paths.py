from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# Encapsulate the date format for partioning
@dataclass(frozen=True)
class UtcPartition:
    date: str  # YYYY-MM-DD
    hour: str  # HH

# Create a UtcPartiion instance with a given datetime, if not given, use utc now
# split the the hour and the date to member variables
def utc_partition(dt: datetime | None = None) -> UtcPartition:
    dt = dt or datetime.now(timezone.utc)
    return UtcPartition(date=dt.strftime("%Y-%m-%d"), hour=dt.strftime("%H"))

# Partiioning for the schedule API
def raw_schedule_key(date: str, hour: str) -> str:
    return f"raw/nhl/schedule/date={date}/hour={hour}/snapshot.json"