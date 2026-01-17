"""
Fetch historical player prop odds from The Odds API.

This module pulls NHL player props (shots on goal, goals, assists, etc.)
from The Odds API's historical endpoints and stores them to S3.

Usage:
    # Backfill from October 2024 to today
    python -m nhl_pipeline.ingestion.fetch_odds_props \
        --start-date 2024-10-04 \
        --end-date 2026-01-04 \
        --market player_shots_on_goal

    # Or run as module
    from nhl_pipeline.ingestion.fetch_odds_props import backfill_props
    backfill_props("2024-10-04", "2026-01-04", market="player_shots_on_goal")
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import requests

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.s3_utils import put_json_to_s3, s3_key_exists
from nhl_pipeline.utils.datetime_utils import utc_now_iso
from nhl_pipeline.utils.paths import raw_odds_player_props_key, raw_odds_events_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# API Configuration
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "icehockey_nhl"
DEFAULT_REGION = "us"
DEFAULT_MARKET = "player_shots_on_goal"

# Rate limiting
REQUEST_DELAY_SECONDS = 0.5  # Be nice to the API
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


@dataclass
class ApiUsage:
    """Track API credit usage."""
    requests_used: int = 0
    requests_remaining: int = 0
    last_cost: int = 0


def _parse_usage_headers(headers: dict) -> ApiUsage:
    """Extract usage info from response headers."""
    return ApiUsage(
        requests_used=int(headers.get("x-requests-used", 0)),
        requests_remaining=int(headers.get("x-requests-remaining", 0)),
        last_cost=int(headers.get("x-requests-last", 0)),
    )

# Given we're beholden to a token amount per month, we need to be careful with requests.
# Set some retry logic to avoid churning tokens.
# Update user on total remaining after each request.
def _make_odds_api_request(
    endpoint: str,
    params: dict[str, Any],
    api_key: str,
) -> tuple[dict[str, Any] | list[Any], ApiUsage]:
    """Make a request to The Odds API with retry logic."""
    params["apiKey"] = api_key
    url = f"{ODDS_API_BASE}/{endpoint}"
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
            
            if resp.status_code == 429:
                # Rate limited - wait and retry
                wait_time = RETRY_DELAY_SECONDS * (attempt + 1)
                logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            
            resp.raise_for_status()
            usage = _parse_usage_headers(resp.headers)
            return resp.json(), usage
            
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Request failed: {e}. Retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise
    
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def fetch_historical_events(
    api_key: str,
    game_date: str,
) -> tuple[list[dict[str, Any]], ApiUsage]:
    """
    Fetch NHL events that were available on a specific historical date.
    
    Args:
        api_key: The Odds API key
        game_date: Date in YYYY-MM-DD format
        
    Returns:
        Tuple of (list of events, API usage info)
    """
    # Query at noon UTC on the game date to capture pre-game lines
    timestamp = f"{game_date}T17:00:00Z"  # 5pm UTC = noon ET
    
    endpoint = f"historical/sports/{SPORT_KEY}/events"
    params = {
        "date": timestamp,
        "dateFormat": "iso",
    }
    
    data, usage = _make_odds_api_request(endpoint, params, api_key)
    
    # Response is wrapped: {"timestamp": ..., "data": [...events...]}
    if isinstance(data, dict) and "data" in data:
        events = data["data"]
    else:
        events = data if isinstance(data, list) else []
    
    return events, usage


def fetch_historical_event_props(
    api_key: str,
    event_id: str,
    game_date: str,
    market: str = DEFAULT_MARKET,
    region: str = DEFAULT_REGION,
) -> tuple[dict[str, Any], ApiUsage]:
    """
    Fetch historical player prop odds for a specific event.
    
    Args:
        api_key: The Odds API key
        event_id: The Odds API event ID
        game_date: Date in YYYY-MM-DD format (for timestamp)
        market: Player prop market (e.g., 'player_shots_on_goal')
        region: Bookmaker region (default: 'us')
        
    Returns:
        Tuple of (event odds data, API usage info)
    """
    # Query at noon UTC on the game date to capture pre-game lines
    timestamp = f"{game_date}T17:00:00Z"
    
    endpoint = f"historical/sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "date": timestamp,
        "regions": region,
        "markets": market,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    
    data, usage = _make_odds_api_request(endpoint, params, api_key)
    
    # Response is wrapped: {"timestamp": ..., "data": {...event...}}
    if isinstance(data, dict) and "data" in data:
        event_data = data["data"]
        # Add metadata
        event_data["_snapshot_timestamp"] = data.get("timestamp")
    else:
        event_data = data
    
    return event_data, usage


def backfill_date(
    api_key: str,
    bucket: str,
    game_date: str,
    market: str = DEFAULT_MARKET,
    skip_existing: bool = True,
) -> tuple[int, int, ApiUsage]:
    """
    Backfill player prop odds for all games on a specific date.
    
    Args:
        api_key: The Odds API key
        bucket: S3 bucket name
        game_date: Date in YYYY-MM-DD format
        market: Player prop market
        skip_existing: Skip events already in S3
        
    Returns:
        Tuple of (events_processed, events_skipped, cumulative_usage)
    """
    logger.info(f"Processing date: {game_date}")
    
    cumulative_usage = ApiUsage()
    events_processed = 0
    events_skipped = 0
    
    # First, get the events for this date
    events_key = raw_odds_events_key(game_date)
    
    if skip_existing and s3_key_exists(bucket=bucket, key=events_key):
        # Load existing events list
        logger.info("  Events list exists, loading from S3...")
        import boto3
        s3 = boto3.client("s3", region_name=get_settings().aws_region)
        obj = s3.get_object(Bucket=bucket, Key=events_key)
        events_data = json.loads(obj["Body"].read().decode("utf-8"))
        events = events_data.get("events", [])
    else:
        # Fetch events from API
        events, usage = fetch_historical_events(api_key, game_date)
        cumulative_usage = usage
        
        # Filter to events on this game date
        events = [
            e for e in events 
            if e.get("commence_time", "").startswith(game_date)
        ]
        
        # Save events list
        events_payload = {
            "extracted_at": utc_now_iso(),
            "game_date": game_date,
            "events": events,
        }
        put_json_to_s3(bucket=bucket, key=events_key, payload=events_payload)
        logger.info(f"  Found {len(events)} events for {game_date}")
        time.sleep(REQUEST_DELAY_SECONDS)
    
    if not events:
        logger.info(f"  No events found for {game_date}")
        return 0, 0, cumulative_usage
    
    # Now fetch props for each event
    for event in events:
        event_id = event.get("id")
        home_team = event.get("home_team", "Unknown")
        away_team = event.get("away_team", "Unknown")
        
        if not event_id:
            continue
        
        props_key = raw_odds_player_props_key(game_date, event_id, market)
        
        if skip_existing and s3_key_exists(bucket=bucket, key=props_key):
            logger.debug(f"    Skipping {away_team} @ {home_team} (already exists)")
            events_skipped += 1
            continue
        
        try:
            props_data, usage = fetch_historical_event_props(
                api_key, event_id, game_date, market
            )
            cumulative_usage = usage
            
            # Wrap with metadata
            payload = {
                "extracted_at": utc_now_iso(),
                "game_date": game_date,
                "event_id": event_id,
                "market": market,
                "home_team": home_team,
                "away_team": away_team,
                "data": props_data,
            }
            
            put_json_to_s3(bucket=bucket, key=props_key, payload=payload)
            events_processed += 1
            
            logger.info(
                f"    ✓ {away_team} @ {home_team} "
                f"(credits remaining: {usage.requests_remaining:,})"
            )
            
            time.sleep(REQUEST_DELAY_SECONDS)
            
        except Exception as e:
            logger.error(f"    ✗ {away_team} @ {home_team}: {e}")
    
    return events_processed, events_skipped, cumulative_usage


def backfill_props(
    start_date: str,
    end_date: str,
    market: str = DEFAULT_MARKET,
    skip_existing: bool = True,
) -> dict[str, Any]:
    """
    Backfill player prop odds for a date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD), inclusive
        end_date: End date (YYYY-MM-DD), inclusive
        market: Player prop market (default: player_shots_on_goal)
        skip_existing: Skip dates/events already processed
        
    Returns:
        Summary dict with stats
    """
    settings = get_settings()
    
    if not settings.odds_api_key:
        raise ValueError(
            "ODDS_API_KEY not configured. "
            "Set it in .env: export ODDS_API_KEY='your_key_here'"
        )
    
    api_key = settings.odds_api_key
    bucket = settings.s3_bucket
    
    logger.info("=" * 60)
    logger.info("NHL Player Props Backfill")
    logger.info(f"  Market: {market}")
    logger.info(f"  Date range: {start_date} to {end_date}")
    logger.info(f"  S3 bucket: {bucket}")
    logger.info(f"  Skip existing: {skip_existing}")
    logger.info("=" * 60)
    
    # Generate date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    total_events = 0
    total_skipped = 0
    total_dates = 0
    final_usage = ApiUsage()
    
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        
        try:
            processed, skipped, usage = backfill_date(
                api_key=api_key,
                bucket=bucket,
                game_date=date_str,
                market=market,
                skip_existing=skip_existing,
            )
            total_events += processed
            total_skipped += skipped
            total_dates += 1
            final_usage = usage
            
        except Exception as e:
            logger.error(f"Failed to process {date_str}: {e}")
        
        current += timedelta(days=1)
    
    logger.info("=" * 60)
    logger.info("Backfill Complete!")
    logger.info(f"  Dates processed: {total_dates}")
    logger.info(f"  Events fetched: {total_events}")
    logger.info(f"  Events skipped: {total_skipped}")
    logger.info(f"  Credits remaining: {final_usage.requests_remaining:,}")
    logger.info("=" * 60)
    
    return {
        "dates_processed": total_dates,
        "events_fetched": total_events,
        "events_skipped": total_skipped,
        "credits_remaining": final_usage.requests_remaining,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Backfill NHL player prop odds from The Odds API"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--market",
        default=DEFAULT_MARKET,
        help=f"Player prop market (default: {DEFAULT_MARKET})",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-fetch events even if they exist in S3",
    )
    
    args = parser.parse_args()
    
    backfill_props(
        start_date=args.start_date,
        end_date=args.end_date,
        market=args.market,
        skip_existing=not args.no_skip_existing,
    )


if __name__ == "__main__":
    main()
