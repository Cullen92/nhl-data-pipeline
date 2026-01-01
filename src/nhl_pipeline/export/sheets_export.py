"""
Export dbt models to Google Sheets for Tableau Public consumption.

Setup:
1. Create a Google Cloud project and enable Sheets API
2. Create a service account and download the JSON key file
3. Set GOOGLE_SHEETS_CREDENTIALS env var to the path of the JSON key file
4. Share your Google Sheet with the service account email (editor access)
5. Set GOOGLE_SHEET_ID env var to the Sheet ID (from the URL)

Environment Variables:
- SNOWFLAKE_ACCOUNT: Snowflake account identifier (required)
- SNOWFLAKE_USER: Snowflake username (required)
- SNOWFLAKE_PASSWORD: Snowflake password (required)
- SNOWFLAKE_DATABASE: Database name (default: NHL)
- SNOWFLAKE_WAREHOUSE: Warehouse name (default: NHL_WH)
- SNOWFLAKE_SCHEMA: Schema name (default: STAGING_SILVER)
- SNOWFLAKE_ROLE: Role name (default: ACCOUNTADMIN)
- GOOGLE_SHEETS_CREDENTIALS: Path to service account JSON key file (optional)
- GOOGLE_SHEET_ID: Google Sheets spreadsheet ID (required)

Usage:
    python -m nhl_pipeline.export.sheets_export
"""

import os
import logging
from typing import Optional

import pandas as pd
import snowflake.connector
import gspread
from gspread_dataframe import set_with_dataframe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Tables to export with their target worksheet names
EXPORTS = [
    {"query": "SELECT * FROM NHL.STAGING_SILVER.PLAYER_SHOT_METRICS", "sheet": "player_shot_metrics"},
    {"query": "SELECT * FROM NHL.STAGING_SILVER.TEAM_SHOT_METRICS", "sheet": "team_shot_metrics"},
    {"query": "SELECT * FROM NHL.STAGING_SILVER.DIM_PLAYER", "sheet": "dim_player"},
    {"query": "SELECT * FROM NHL.STAGING_SILVER.DIM_TEAM", "sheet": "dim_team"},
]


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "NHL"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NHL_WH"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "STAGING_SILVER"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def query_to_dataframe(conn: snowflake.connector.SnowflakeConnection, query: str) -> pd.DataFrame:
    """Execute query and return results as DataFrame."""
    logger.info(f"Executing: {query[:80]}...")
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        logger.info(f"  Retrieved {len(df)} rows")
        return df
    finally:
        cursor.close()


def get_google_sheets_client() -> gspread.Client:
    """Authenticate with Google Sheets using service account."""
    creds_path = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if creds_path:
        return gspread.service_account(filename=creds_path)
    else:
        # Try default credentials location (~/.config/gspread/service_account.json)
        return gspread.service_account()


def export_to_sheet(
    gc: gspread.Client,
    spreadsheet_id: str,
    df: pd.DataFrame,
    worksheet_name: str,
) -> None:
    """Export DataFrame to a worksheet in the specified Google Sheet."""
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # Get or create worksheet
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()  # Clear existing data
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name, 
            rows=len(df) + 1, 
            cols=len(df.columns)
        )
    
    # Write DataFrame to sheet
    set_with_dataframe(worksheet, df)
    logger.info(f"  Exported to worksheet: {worksheet_name}")


def main(spreadsheet_id: Optional[str] = None) -> None:
    """Main export function."""
    sheet_id = spreadsheet_id or os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError(
            "No Google Sheet ID provided. "
            "Set GOOGLE_SHEET_ID env var or pass spreadsheet_id parameter."
        )
    
    logger.info("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    
    logger.info("Authenticating with Google Sheets...")
    gc = get_google_sheets_client()
    
    try:
        for export in EXPORTS:
            logger.info(f"Exporting {export['sheet']}...")
            df = query_to_dataframe(conn, export["query"])
            export_to_sheet(gc, sheet_id, df, export["sheet"])
        
        logger.info("Export complete!")
        logger.info(f"View your data: https://docs.google.com/spreadsheets/d/{sheet_id}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
