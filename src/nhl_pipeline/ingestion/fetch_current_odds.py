"""
Fetch current/upcoming NHL player prop odds from The Odds API.

This module fetches live odds for upcoming games (next 48 hours) using
the current endpoints (not historical). Used by the daily odds ingestion DAG.

Usage:
    from nhl_pipeline.ingestion.fetch_current_odds import (
        fetch_upcoming_events,
        fetch_current_event_odds,
        ingest_upcoming_odds_to_s3,
    )
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.api_utils import make_odds_api_request
from nhl_pipeline.ingestion.s3_utils import put_json_to_s3
from nhl_pipeline.utils.datetime_utils import utc_now_iso

# API Configuration
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "icehockey_nhl"
DEFAULT_REGION = "us"
DEFAULT_MARKET = "player_shots_on_goal"

# Rate limiting
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


def fetch_upcoming_events(api_key: str) -> tuple[list[dict], dict]:
    """
    Fetch upcoming NHL events from The Odds API.
    
    Args:
        api_key: The Odds API key
        
    Returns:
        Tuple of (list of events, API usage info)
    """
    endpoint = f"sports/{SPORT_KEY}/events"
    params = {"dateFormat": "iso"}
    
    data, usage = make_odds_api_request(
        ODDS_API_BASE, endpoint, params, api_key, MAX_RETRIES, RETRY_DELAY_SECONDS
    )
    events = data if isinstance(data, list) else []
    
    # Convert ApiUsage dataclass to dict for backward compatibility
    usage_dict = {
        "requests_used": usage.requests_used,
        "requests_remaining": usage.requests_remaining,
        "last_cost": usage.last_cost,
    }
    
    return events, usage_dict


def fetch_current_event_odds(
    api_key: str,
    event_id: str,
    market: str,
    region: str = DEFAULT_REGION,
) -> tuple[dict[str, Any], dict]:
    """
    Fetch current odds for a specific NHL event.
    
    Args:
        api_key: The Odds API key
        event_id: The Odds API event ID
        market: Player prop market (e.g., 'player_shots_on_goal')
        region: Bookmaker region (default: 'us')
        
    Returns:
        Tuple of (event odds data, API usage info)
    """
    endpoint = f"sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "regions": region,
        "markets": market,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    
    data, usage = make_odds_api_request(
        ODDS_API_BASE, endpoint, params, api_key, MAX_RETRIES, RETRY_DELAY_SECONDS
    )
    
    # Convert ApiUsage dataclass to dict for backward compatibility
    usage_dict = {
        "requests_used": usage.requests_used,
        "requests_remaining": usage.requests_remaining,
        "last_cost": usage.last_cost,
    }
    
    return data, usage_dict


def ingest_upcoming_odds_to_s3(
    api_key: str,
    execution_timestamp: str,
    market: str = DEFAULT_MARKET,
    hours_ahead: int = 48,
) -> dict[str, Any]:
    """
    Fetch current odds for upcoming games and upload to S3.
    
    Args:
        api_key: The Odds API key
        execution_timestamp: Airflow execution timestamp (ISO format)
        market: Player prop market to fetch
        hours_ahead: How many hours ahead to look for games
        
    Returns:
        Summary dict with stats
    """
    settings = get_settings()
    bucket = settings.s3_bucket
    execution_dt = datetime.fromisoformat(execution_timestamp.replace("Z", "+00:00"))
    
    print("=" * 80)
    print("NHL Current Odds Ingestion")
    print(f"  Execution time: {execution_timestamp}")
    print(f"  Market: {market}")
    print(f"  Hours ahead: {hours_ahead}")
    print(f"  S3 bucket: {bucket}")
    print("=" * 80)
    
    # Fetch upcoming events
    print("\nFetching upcoming NHL events...")
    events, usage = fetch_upcoming_events(api_key)
    print(f"Found {len(events)} upcoming events")
    print(f"API credits remaining: {usage['requests_remaining']:,}")
    
    if not events:
        print("No upcoming events found.")
        return {"events_processed": 0, "credits_remaining": usage["requests_remaining"]}
    
    # Filter to specified time window
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=hours_ahead)
    
    relevant_events = []
    for event in events:
        commence_time = event.get("commence_time", "")
        if not commence_time:
            continue
        
        commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        if now <= commence_dt <= cutoff:
            relevant_events.append(event)
    
    print(f"\n{len(relevant_events)} events in next {hours_ahead} hours")
    
    # Fetch odds for each event
    total_fetched = 0
    credits_remaining = usage["requests_remaining"]
    
    for event in relevant_events:
        event_id = event.get("id")
        home_team = event.get("home_team", "Unknown")
        away_team = event.get("away_team", "Unknown")
        commence_time = event.get("commence_time", "")
        
        if not event_id:
            continue
        
        try:
            # Fetch current odds
            odds_data, usage = fetch_current_event_odds(api_key, event_id, market)
            credits_remaining = usage["requests_remaining"]
            
            # Determine partition date (use commence date)
            commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            partition_date = commence_dt.strftime("%Y-%m-%d")
            
            # Build S3 key: raw/odds/player_props/market=X/date=Y/event_Z_snapshot_TIMESTAMP.json
            timestamp = execution_dt.strftime("%Y%m%d_%H%M%S")
            s3_key = f"raw/odds/player_props/market={market}/date={partition_date}/event_{event_id}_snapshot_{timestamp}.json"
            
            payload = {
                "extracted_at": utc_now_iso(),
                "execution_date": execution_timestamp,
                "partition_date": partition_date,
                "event_id": event_id,
                "market": market,
                "home_team": home_team,
                "away_team": away_team,
                "commence_time": commence_time,
                "data": odds_data,
            }
            
            put_json_to_s3(bucket=bucket, key=s3_key, payload=payload)
            total_fetched += 1
            
            print(f"  ✓ {commence_time[:10]} {commence_time[11:16]} - {away_team} @ {home_team} (credits: {credits_remaining:,})")
            
            time.sleep(REQUEST_DELAY_SECONDS)
            
        except Exception as e:
            print(f"  ✗ {away_team} @ {home_team}: {e}")
    
    print("\n" + "=" * 80)
    print("Current Odds Ingestion Complete!")
    print(f"  Events processed: {total_fetched}")
    print(f"  Credits remaining: {credits_remaining:,}")
    print("=" * 80)
    
    return {
        "events_processed": total_fetched,
        "credits_remaining": credits_remaining,
    }