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
from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from nhl_pipeline.ingestion.fetch_current_odds import ingest_upcoming_odds_to_s3

# Default market can be overridden via environment variable
DEFAULT_MARKET = "player_shots_on_goal"


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
        api_key = _get_api_key()
        market = os.getenv("ODDS_DAILY_MARKET", DEFAULT_MARKET)
        
        return ingest_upcoming_odds_to_s3(
            api_key=api_key,
            execution_timestamp=ts,
            market=market,
            hours_ahead=48,
        )

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
