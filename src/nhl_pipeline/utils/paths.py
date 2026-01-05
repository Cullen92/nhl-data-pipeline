from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# Encapsulate the date format for partioning
@dataclass(frozen=True)
class UtcPartition:
    date: str  # YYYY-MM-DD
    hour: str  # HH

# Create a UtcPartition instance with a given datetime, if not given, use utc now
# split the the hour and the date to member variables
def utc_partition(dt: datetime | None = None) -> UtcPartition:
    dt = dt or datetime.now(timezone.utc)
    return UtcPartition(date=dt.strftime("%Y-%m-%d"), hour=dt.strftime("%H"))

def snapshot_filename(date: str, hour: str) -> str:
    """Standard snapshot object name.

    We keep the folder partitioning as `date=YYYY-MM-DD/hour=HH/` and encode
    the same values into the filename for uniqueness + easier manual browsing.
    """
    try:
        year, month, day = date.split("-", 2)
    except ValueError as e:
        raise ValueError(f"Expected date in YYYY-MM-DD format, got: {date!r}") from e

    if not (isinstance(hour, str) and len(hour) == 2 and hour.isdigit()):
        raise ValueError(f"Expected hour in HH format, got: {hour!r}")

    return f"snapshot_{year}_{month}_{day}_{hour}.json"


# Partitioning for the schedule API
def raw_schedule_key(date: str, hour: str) -> str:
    return f"raw/nhl/schedule/date={date}/hour={hour}/{snapshot_filename(date, hour)}"


def raw_game_boxscore_key(date: str, hour: str, game_id: int | str) -> str:
    return (
        f"raw/nhl/game_boxscore/date={date}/hour={hour}/game_id={game_id}/"
        f"{snapshot_filename(date, hour)}"
    )


def raw_game_pbp_key(date: str, hour: str, game_id: int | str) -> str:
    return (
        f"raw/nhl/game_pbp/date={date}/hour={hour}/game_id={game_id}/"
        f"{snapshot_filename(date, hour)}"
    )


def raw_stats_skater_summary_key(
    date: str,
    hour: str,
    season_id: int | str,
    game_type_id: int | str,
    start: int,
) -> str:
    return (
        f"raw/nhl/stats/skater_summary/date={date}/hour={hour}/"
        f"season_id={season_id}/game_type_id={game_type_id}/start={start}/"
        f"{snapshot_filename(date, hour)}"
    )


def raw_stats_skater_timeonice_key(
    date: str,
    hour: str,
    season_id: int | str,
    game_type_id: int | str,
    start: int,
) -> str:
    return (
        f"raw/nhl/stats/skater_timeonice/date={date}/hour={hour}/"
        f"season_id={season_id}/game_type_id={game_type_id}/start={start}/"
        f"{snapshot_filename(date, hour)}"
    )


def raw_stats_skater_powerplay_key(
    date: str,
    hour: str,
    season_id: int | str,
    game_type_id: int | str,
    start: int,
) -> str:
    return (
        f"raw/nhl/stats/skater_powerplay/date={date}/hour={hour}/"
        f"season_id={season_id}/game_type_id={game_type_id}/start={start}/"
        f"{snapshot_filename(date, hour)}"
    )


def raw_meta_backfill_gamecenter_success_key(date: str) -> str:
    """Marker written when a gamecenter backfill date completes successfully."""
    return f"raw/nhl/_meta/backfill/gamecenter/date={date}/_SUCCESS.json"


def raw_odds_player_props_key(game_date: str, event_id: str, market: str) -> str:
    """S3 key for historical player prop odds from The Odds API.
    
    Args:
        game_date: Game date in YYYY-MM-DD format
        event_id: The Odds API event ID
        market: Market type (e.g., 'player_shots_on_goal')
    """
    return (
        f"raw/odds/player_props/market={market}/date={game_date}/"
        f"event_{event_id}.json"
    )


def raw_odds_events_key(game_date: str) -> str:
    """S3 key for historical NHL events from The Odds API."""
    return f"raw/odds/events/date={game_date}/events.json"