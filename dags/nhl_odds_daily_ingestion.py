"""
Airflow DAG for daily ingestion of UPCOMING NHL player prop odds.

This DAG runs daily to fetch current/upcoming player prop odds for today's and
tomorrow's games. Unlike the backfill DAG which fetches historical odds at specific
timestamps, this DAG fetches the CURRENT live odds.

Required Airflow Variables:
    - ODDS_API_KEY: Your API key from the-odds-api.com (mark as encrypted!)
    
Schedule:
    - Runs 3x daily: 10am, 2pm, 6pm ET
    - Fetches odds for games in the next 48 hours
    - Uses current endpoint (not historical)
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.models import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import requests

from nhl_pipeline.config import get_settings
from nhl_pipeline.ingestion.s3_utils import put_json_to_s3
from nhl_pipeline.utils.datetime_utils import utc_now_iso


# API Configuration
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "icehockey_nhl"
DEFAULT_REGION = "us"
DEFAULT_MARKET = "player_shots_on_goal"
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


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
    raise ValueError("ODDS_API_KEY not configured.")


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


def fetch_upcoming_events(api_key: str) -> tuple[list[dict], dict]:
    """Fetch upcoming NHL events (next 48 hours)."""
    endpoint = f"sports/{SPORT_KEY}/events"
    params = {
        "dateFormat": "iso",
    }
    
    data, usage = _make_odds_api_request(endpoint, params, api_key)
    events = data if isinstance(data, list) else []
    
    return events, usage


def fetch_event_odds(
    api_key: str,
    event_id: str,
    market: str,
    region: str = DEFAULT_REGION,
) -> tuple[dict[str, Any], dict]:
    """Fetch current odds for a specific event."""
    endpoint = f"sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "regions": region,
        "markets": market,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    
    data, usage = _make_odds_api_request(endpoint, params, api_key)
    return data, usage


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nhl_odds_daily_ingestion",
    default_args=default_args,
    description="Daily ingestion of upcoming NHL player prop odds",
    schedule="0 10,14,18 * * *",  # 10am, 2pm, 6pm ET daily
    catchup=False,
    tags=["nhl", "odds", "daily"],
) as dag:

    def ingest_upcoming_odds(ts: str, **context):
        """Fetch current odds for upcoming games."""
        settings = get_settings()
        api_key = _get_api_key()
        bucket = settings.s3_bucket
        
        market = os.getenv("ODDS_DAILY_MARKET", DEFAULT_MARKET)
        execution_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        
        print("=" * 80)
        print("NHL Daily Odds Ingestion")
        print(f"  Execution time: {ts}")
        print(f"  Market: {market}")
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
        
        # Filter to next 48 hours
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=48)
        
        relevant_events = []
        for event in events:
            commence_time = event.get("commence_time", "")
            if not commence_time:
                continue
            
            commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            if now <= commence_dt <= cutoff:
                relevant_events.append(event)
        
        print(f"\n{len(relevant_events)} events in next 48 hours")
        
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
                odds_data, usage = fetch_event_odds(api_key, event_id, market)
                credits_remaining = usage["requests_remaining"]
                
                # Determine partition date (use commence date)
                commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                partition_date = commence_dt.strftime("%Y-%m-%d")
                
                # Build S3 key: raw/odds/player_props/market=X/date=Y/event_Z_snapshot_TIMESTAMP.json
                timestamp = execution_dt.strftime("%Y%m%d_%H%M%S")
                s3_key = f"raw/odds/player_props/market={market}/date={partition_date}/event_{event_id}_snapshot_{timestamp}.json"
                
                payload = {
                    "extracted_at": utc_now_iso(),
                    "execution_date": ts,
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
        print("Daily Odds Ingestion Complete!")
        print(f"  Events processed: {total_fetched}")
        print(f"  Credits remaining: {credits_remaining:,}")
        print("=" * 80)
        
        return {
            "events_processed": total_fetched,
            "credits_remaining": credits_remaining,
        }

    task_ingest_odds = PythonOperator(
        task_id="ingest_upcoming_odds",
        python_callable=ingest_upcoming_odds,
        op_kwargs={"ts": "{{ ts }}"},
    )

    # Load odds data into Snowflake
    # This loads from the raw/odds/player_props/ path in S3
    load_odds = SQLExecuteQueryOperator(
        task_id="load_odds_snowflake",
        conn_id="snowflake_default",
        sql="""
            COPY INTO NHL.RAW_ODDS.PLAYER_PROPS (payload, s3_key, partition_date, event_id, market, ingest_ts)
            FROM (
                SELECT 
                    $1,
                    METADATA$FILENAME,
                    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e')),
                    REGEXP_SUBSTR(METADATA$FILENAME, 'event_([a-z0-9]+)', 1, 1, 'e'),
                    REGEXP_SUBSTR(METADATA$FILENAME, 'market=([^/]+)', 1, 1, 'e'),
                    CURRENT_TIMESTAMP()
                FROM @NHL.RAW_ODDS.ODDS_S3_STAGE/player_props/
            )
            FILE_FORMAT=(TYPE=JSON)
            PATTERN='.*\.json$'
            ON_ERROR='CONTINUE';
        """,
        autocommit=True,
    )

    # dbt Cloud - trigger odds job and wait for completion
    # Uses separate job that runs with --select tag:odds
    run_dbt_odds_models = DbtCloudRunJobOperator(
        task_id="run_dbt_odds_models",
        job_id="{{ var.value.DBT_CLOUD_ODDS_JOB_ID }}",
        check_interval=30,
        timeout=600,
        wait_for_termination=True,
    )

    # Dependencies
    task_ingest_odds >> load_odds >> run_dbt_odds_models
