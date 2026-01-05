"""
Airflow DAG to backfill NHL player prop odds from The Odds API.

This DAG fetches historical player prop betting lines (shots on goal, goals, etc.)
and stores them to S3 for later analysis.

Required Airflow Variables:
    - ODDS_API_KEY: Your API key from the-odds-api.com (mark as encrypted!)
    
Optional Environment Variables / Airflow Variables:
    - ODDS_BACKFILL_START_DATE: Start date (YYYY-MM-DD), default: 2024-10-04
    - ODDS_BACKFILL_END_DATE: End date (YYYY-MM-DD), default: today
    - ODDS_BACKFILL_MARKET: Player prop market, default: player_shots_on_goal
    - ODDS_BACKFILL_FORCE: If true, re-fetch existing data

Usage:
    1. Add ODDS_API_KEY as an encrypted Airflow Variable
    2. Optionally set ODDS_BACKFILL_START_DATE and ODDS_BACKFILL_END_DATE
    3. Trigger the DAG manually
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.models import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

import requests

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.s3_utils import put_json_to_s3, s3_key_exists
from nhl_pipeline.utils.datetime_utils import utc_now_iso
from nhl_pipeline.utils.paths import raw_odds_player_props_key, raw_odds_events_key


# API Configuration
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "icehockey_nhl"
DEFAULT_REGION = "us"
DEFAULT_MARKET = "player_shots_on_goal"
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


def _env(name: str, default: str) -> str:
    """Get value from environment or Airflow Variable."""
    env_val = os.getenv(name)
    if env_val:
        return env_val
    try:
        return Variable.get(name, default_var=default)
    except Exception:
        return default


def _parse_date_yyyy_mm_dd(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _get_api_key() -> str:
    """Get Odds API key from Airflow Variable or environment."""
    api_key = os.getenv("ODDS_API_KEY")
    if api_key:
        return api_key
    try:
        api_key = Variable.get("ODDS_API_KEY")
        if api_key:
            return api_key
    except Exception as e:
        raise ValueError(
            f"ODDS_API_KEY not configured. Add it as an encrypted Airflow Variable. Error: {e}"
        )
    raise ValueError("ODDS_API_KEY not configured. Add it as an encrypted Airflow Variable.")


def _make_odds_api_request(
    endpoint: str,
    params: dict[str, Any],
    api_key: str,
) -> tuple[dict[str, Any] | list[Any], dict[str, int]]:
    """Make a request to The Odds API with retry logic."""
    params["apiKey"] = api_key
    url = f"{ODDS_API_BASE}/{endpoint}"
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
            
            if resp.status_code == 429:
                wait_time = RETRY_DELAY_SECONDS * (attempt + 1)
                print(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            
            resp.raise_for_status()
            usage = {
                "requests_used": int(resp.headers.get("x-requests-used", 0)),
                "requests_remaining": int(resp.headers.get("x-requests-remaining", 0)),
                "last_cost": int(resp.headers.get("x-requests-last", 0)),
            }
            return resp.json(), usage
            
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Request failed: {e}. Retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise
    
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def fetch_historical_events(api_key: str, game_date: str) -> tuple[list[dict], dict]:
    """Fetch NHL events that were available on a specific historical date.
    
    We query at 11pm UTC (6pm ET) - late enough that all games are scheduled,
    and we use commenceTimeFrom/To to filter games by their start time.
    
    Note: Games that started earlier in the day will still appear in the snapshot
    as long as they haven't completed - the API includes live games.
    """
    # Query at 11pm UTC (6pm ET) - full schedule is posted by then
    timestamp = f"{game_date}T23:00:00Z"
    
    # Filter games starting from midnight UTC to 8am UTC next day
    # This captures all games on this calendar date (in ET timezone)
    from datetime import datetime, timedelta
    game_dt = datetime.strptime(game_date, "%Y-%m-%d")
    next_day = (game_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    
    endpoint = f"historical/sports/{SPORT_KEY}/events"
    params = {
        "date": timestamp,
        "dateFormat": "iso",
        "commenceTimeFrom": f"{game_date}T00:00:00Z",
        "commenceTimeTo": f"{next_day}T07:59:59Z",  # Captures late west coast games
    }
    
    data, usage = _make_odds_api_request(endpoint, params, api_key)
    
    if isinstance(data, dict) and "data" in data:
        events = data["data"]
    else:
        events = data if isinstance(data, list) else []
    
    return events, usage


def fetch_historical_event_props(
    api_key: str,
    event_id: str,
    commence_time: str,  # ISO format commence time from event data
    market: str,
    region: str = DEFAULT_REGION,
) -> tuple[dict[str, Any], dict]:
    """Fetch historical player prop odds for a specific event.
    
    We query 1 hour before the event's commence_time. This ensures:
    1. Props are posted (typically available 1-24 hours before game)
    2. Game hasn't started yet (so pregame lines are available)
    3. We capture near-closing lines
    
    This handles matinee vs evening games correctly.
    """
    from datetime import datetime, timedelta
    
    # Parse commence_time and query 1 hour before
    if commence_time.endswith("Z"):
        commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    else:
        commence_dt = datetime.fromisoformat(commence_time)
    
    query_dt = commence_dt - timedelta(hours=1)
    timestamp = query_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    endpoint = f"historical/sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "date": timestamp,
        "regions": region,
        "markets": market,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    
    data, usage = _make_odds_api_request(endpoint, params, api_key)
    
    if isinstance(data, dict) and "data" in data:
        event_data = data["data"]
        event_data["_snapshot_timestamp"] = data.get("timestamp")
    else:
        event_data = data
    
    return event_data, usage


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nhl_odds_props_backfill",
    default_args=default_args,
    description="Backfill NHL player prop odds (SOG, goals, etc.) from The Odds API",
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["nhl", "odds", "backfill"],
) as dag:

    def backfill_odds_props(**context):
        """Main backfill function."""
        settings = get_settings()
        api_key = _get_api_key()
        bucket = settings.s3_bucket
        
        # Configuration
        start_date = _env("ODDS_BACKFILL_START_DATE", "2024-10-04")
        end_date = _env("ODDS_BACKFILL_END_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        market = _env("ODDS_BACKFILL_MARKET", DEFAULT_MARKET)
        
        force_raw = _env("ODDS_BACKFILL_FORCE", "false")
        force = force_raw.strip().lower() in {"1", "true", "yes"}
        skip_existing = not force
        
        start_dt = _parse_date_yyyy_mm_dd(start_date)
        end_dt = _parse_date_yyyy_mm_dd(end_date)
        
        print("=" * 60)
        print("NHL Player Props Backfill (MWAA)")
        print(f"  Market: {market}")
        print(f"  Date range: {start_date} to {end_date}")
        print(f"  S3 bucket: {bucket}")
        print(f"  Skip existing: {skip_existing}")
        print("=" * 60)
        
        total_events = 0
        total_skipped = 0
        total_dates = 0
        credits_remaining = 0
        
        day = start_dt
        while day <= end_dt:
            day_str = day.strftime("%Y-%m-%d")
            print(f"\nProcessing date: {day_str}")
            
            # Check/fetch events for this date
            events_key = raw_odds_events_key(day_str)
            
            if skip_existing and s3_key_exists(bucket=bucket, key=events_key):
                # Load existing events list from S3
                print("  Events list exists, loading from S3...")
                import boto3
                s3 = boto3.client("s3", region_name=settings.aws_region)
                obj = s3.get_object(Bucket=bucket, Key=events_key)
                events_data = json.loads(obj["Body"].read().decode("utf-8"))
                events = events_data.get("events", [])
            else:
                # Fetch from API
                events, usage = fetch_historical_events(api_key, day_str)
                credits_remaining = usage["requests_remaining"]
                
                # Save events list
                events_payload = {
                    "extracted_at": utc_now_iso(),
                    "game_date": day_str,
                    "events": events,
                }
                put_json_to_s3(bucket=bucket, key=events_key, payload=events_payload)
                print(f"  Found {len(events)} events for {day_str}")
                time.sleep(REQUEST_DELAY_SECONDS)
            
            if not events:
                print(f"  No events found for {day_str}")
                day += timedelta(days=1)
                total_dates += 1
                continue
            
            # Fetch props for each event
            for event in events:
                event_id = event.get("id")
                home_team = event.get("home_team", "Unknown")
                away_team = event.get("away_team", "Unknown")
                commence_time = event.get("commence_time", "")
                
                if not event_id or not commence_time:
                    continue
                
                props_key = raw_odds_player_props_key(day_str, event_id, market)
                
                if skip_existing and s3_key_exists(bucket=bucket, key=props_key):
                    print(f"    Skipping {away_team} @ {home_team} (exists)")
                    total_skipped += 1
                    continue
                
                try:
                    props_data, usage = fetch_historical_event_props(
                        api_key, event_id, commence_time, market
                    )
                    credits_remaining = usage["requests_remaining"]
                    
                    payload = {
                        "extracted_at": utc_now_iso(),
                        "game_date": day_str,
                        "event_id": event_id,
                        "market": market,
                        "home_team": home_team,
                        "away_team": away_team,
                        "data": props_data,
                    }
                    
                    put_json_to_s3(bucket=bucket, key=props_key, payload=payload)
                    total_events += 1
                    
                    print(f"    ✓ {away_team} @ {home_team} (credits: {credits_remaining:,})")
                    time.sleep(REQUEST_DELAY_SECONDS)
                    
                except Exception as e:
                    print(f"    ✗ {away_team} @ {home_team}: {e}")
            
            total_dates += 1
            day += timedelta(days=1)
        
        print("\n" + "=" * 60)
        print("Backfill Complete!")
        print(f"  Dates processed: {total_dates}")
        print(f"  Events fetched: {total_events}")
        print(f"  Events skipped: {total_skipped}")
        print(f"  Credits remaining: {credits_remaining:,}")
        print("=" * 60)
        
        return {
            "dates_processed": total_dates,
            "events_fetched": total_events,
            "events_skipped": total_skipped,
            "credits_remaining": credits_remaining,
        }

    task_backfill = PythonOperator(
        task_id="backfill_odds_props",
        python_callable=backfill_odds_props,
    )
