from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def parse_airflow_ts(ts: str) -> datetime:
    """Parse Airflow {{ ts }} into a timezone-aware UTC datetime."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def partition_date_from_ts(ts: str) -> str:
    return parse_airflow_ts(ts).strftime("%Y-%m-%d")


def _parse_utc_dt(value: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iter_schedule_games(payload: Any) -> list[dict[str, Any]]:
    """Return game dicts from schedule payload.

    Both `schedule/now` and `schedule/YYYY-MM-DD` return a dict with `gameWeek`.
    """

    if not isinstance(payload, dict):
        return []
    game_week = payload.get("gameWeek")
    if not isinstance(game_week, list):
        return []

    games: list[dict[str, Any]] = []
    for day in game_week:
        if not isinstance(day, dict):
            continue
        day_games = day.get("games")
        if not isinstance(day_games, list):
            continue
        for g in day_games:
            if isinstance(g, dict):
                games.append(g)
    return games


def extract_game_ids(
    payload: Any,
    *,
    partition_dt: datetime,
    lookback_days: int = 3,
    only_final: bool = True,
    max_games: int = 30,
) -> list[int]:
    """Select which gameIds to ingest.

    Default behavior: FINAL-only within a lookback window.
    """

    window_start = partition_dt - timedelta(days=lookback_days)
    window_end = partition_dt + timedelta(days=1)

    final_states = {"OFF", "OVER", "FINAL", "FINAL OT", "FINAL SO"}

    selected: list[int] = []
    for game in iter_schedule_games(payload):
        game_id = game.get("id")
        if not isinstance(game_id, int) or len(str(game_id)) != 10:
            continue

        start_time_utc = _parse_utc_dt(game.get("startTimeUTC", ""))
        if start_time_utc is not None:
            if not (window_start <= start_time_utc < window_end):
                continue

        if only_final:
            state = (
                game.get("gameState")
                or game.get("gameStatus")
                or game.get("gameScheduleState")
                or game.get("status")
            )
            if state is not None and str(state).upper() not in final_states:
                continue

        selected.append(game_id)
        if len(selected) >= max_games:
            break

    # De-dupe while keeping order.
    seen: set[int] = set()
    deduped: list[int] = []
    for gid in selected:
        if gid not in seen:
            seen.add(gid)
            deduped.append(gid)
    return deduped
