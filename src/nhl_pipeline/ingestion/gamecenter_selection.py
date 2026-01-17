"""
Utilities for selecting and filtering NHL games from schedule data.

Provides functions to extract game IDs from schedule payloads,
filter by game state (final games only), and handle time windows.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from nhl_pipeline.utils.datetime_utils import parse_airflow_ts, parse_utc_dt


def partition_date_from_ts(ts: str) -> str:
    return parse_airflow_ts(ts).strftime("%Y-%m-%d")

# Note: Payload contains teams, venue, officials, broadcasts, ticket links, and more.

# Input: {"gameWeek": [{"games": [game1, game2]}, {"games": [game3]}]}
# Output: [game1, game2, game3]  # Flat list
def iter_schedule_games(payload: Any) -> list[dict[str, Any]]:
    """Return game dicts from schedule payload.

    Both `schedule/now` and `schedule/YYYY-MM-DD` return a dict with `gameWeek`.
    """

    # Get game week
    if not isinstance(payload, dict):
        return []
    game_week = payload.get("gameWeek")
    if not isinstance(game_week, list):
        return []

    # Get day in the week
    games: list[dict[str, Any]] = []
    for day in game_week:
        if not isinstance(day, dict):
            continue
        #Get games in the day
        day_games = day.get("games")
        if not isinstance(day_games, list):
            continue
        #Get individual game
        for g in day_games:
            if isinstance(g, dict):
                games.append(g)
    return games


def extract_final_game_ids(payload: Any, *, max_games: int = 30) -> list[int]:
    """Extract game IDs for finished games from schedule payload."""
    
    final_states = {"OFF", "OVER", "FINAL", "FINAL OT", "FINAL SO"}
    selected: list[int] = []
    
    for game in iter_schedule_games(payload):
        game_id = game.get("id")
        if not isinstance(game_id, int) or len(str(game_id)) != 10:
            continue
            
        state = (game.get("gameState") or game.get("gameStatus") or 
                game.get("gameScheduleState") or game.get("status"))
        if state is None or str(state).upper() not in final_states:
            continue
            
        selected.append(game_id)
        if len(selected) >= max_games:
            break
    
    # Dedupe while preserving order
    return list(dict.fromkeys(selected))
