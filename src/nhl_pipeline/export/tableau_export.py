"""
Export shot location data to CSV files for Tableau Public heatmaps.

This exports aggregated shot location data that can be used with a rink
background image in Tableau to create shot heatmaps.

Output files (saved to exports/ folder):
- player_shot_locations.csv: Per-player shot locations by coordinate bin
- team_shot_locations.csv: Per-team shot locations (offense and defense)
- fact_shot_events.csv: Raw shot events (large file, optional)

Environment Variables:
- SNOWFLAKE_ACCOUNT: Snowflake account identifier (required)
- SNOWFLAKE_USER: Snowflake username (required)
- SNOWFLAKE_PASSWORD: Snowflake password (required)
- SNOWFLAKE_DATABASE: Database name (default: NHL)
- SNOWFLAKE_WAREHOUSE: Warehouse name (default: NHL_WH)
- SNOWFLAKE_SCHEMA: Schema name (default: STAGING_SILVER)
- SNOWFLAKE_ROLE: Role name (default: ACCOUNTADMIN)

Usage:
    python -m nhl_pipeline.export.tableau_export
    python -m nhl_pipeline.export.tableau_export --include-raw  # Include raw events
"""

import os
import logging
import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
import snowflake.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Export directory
EXPORT_DIR = Path(__file__).parent.parent.parent.parent / "exports"

# Queries for Tableau exports
TABLEAU_EXPORTS = [
    {
        "name": "player_shot_locations",
        "query": """
            SELECT 
                shooter_player_id,
                shooter_name,
                shooter_position_code,
                shooter_position_type,
                team_abbrev,
                team_name,
                season,
                x_bin,
                y_bin,
                total_shots,
                goals,
                shots_on_goal,
                missed_shots,
                shooting_pct,
                wrist_shots,
                slap_shots,
                snap_shots,
                backhand_shots,
                tip_in_shots,
                deflected_shots,
                wrap_around_shots
            FROM NHL.STAGING_SILVER.PLAYER_SHOT_LOCATIONS
        """,
        "description": "Player shot locations by coordinate bin for heatmap",
    },
    {
        "name": "team_shot_locations",
        "query": """
            SELECT 
                team_id,
                team_abbrev,
                team_name,
                season,
                shot_context,
                x_bin,
                y_bin,
                total_shots,
                goals,
                shots_on_goal,
                missed_shots,
                shooting_pct,
                forward_shots,
                defense_shots,
                wrist_shots,
                slap_shots,
                snap_shots,
                backhand_shots
            FROM NHL.STAGING_SILVER.TEAM_SHOT_LOCATIONS
        """,
        "description": "Team shot locations (offense/defense) by coordinate bin",
    },
]

# Raw events export (optional, can be large)
RAW_EVENTS_EXPORT = {
    "name": "fact_shot_events",
    "query": """
        SELECT 
            game_id,
            event_id,
            game_date,
            season,
            period,
            time_in_period,
            x_coord,
            y_coord,
            x_coord_normalized,
            y_coord_normalized,
            shooter_player_id,
            shooter_name,
            shooter_position_type,
            shooting_team_id,
            defending_team_id,
            shot_result,
            shot_type,
            is_goal,
            home_team_abbrev,
            away_team_abbrev
        FROM NHL.STAGING_SILVER.FACT_SHOT_EVENTS
    """,
    "description": "Raw shot events with coordinates (large file)",
}


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


def query_to_dataframe(
    conn: snowflake.connector.SnowflakeConnection, query: str
) -> pd.DataFrame:
    """Execute query and return results as DataFrame."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        return df
    finally:
        cursor.close()


def export_to_csv(
    conn: snowflake.connector.SnowflakeConnection,
    export_config: dict,
    output_dir: Path,
) -> Path:
    """Export query results to CSV file."""
    name = export_config["name"]
    query = export_config["query"]
    description = export_config.get("description", "")

    logger.info(f"Exporting {name}: {description}")
    df = query_to_dataframe(conn, query)

    output_path = output_dir / f"{name}.csv"
    df.to_csv(output_path, index=False)

    logger.info(f"  Wrote {len(df):,} rows to {output_path}")
    return output_path


def main(include_raw: bool = False) -> None:
    """Export all shot location data to CSV for Tableau."""
    # Create export directory
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Connecting to Snowflake...")
    conn = get_snowflake_connection()

    try:
        exports = TABLEAU_EXPORTS.copy()
        if include_raw:
            exports.append(RAW_EVENTS_EXPORT)

        exported_files = []
        for export_config in exports:
            try:
                output_path = export_to_csv(conn, export_config, EXPORT_DIR)
                exported_files.append(output_path)
            except Exception as e:
                logger.error(f"Failed to export {export_config['name']}: {e}")

        logger.info(f"\n{'='*60}")
        logger.info("Export complete! Files saved to:")
        for f in exported_files:
            logger.info(f"  - {f}")

        logger.info(f"\n{'='*60}")
        logger.info("TABLEAU SETUP INSTRUCTIONS:")
        logger.info("1. Open Tableau Public and connect to CSV files")
        logger.info("2. Add a background image: Map → Background Images → Add Image")
        logger.info("3. Use an NHL rink image (200ft x 85ft dimensions)")
        logger.info("4. Map X field to x_bin (range: 0 to 100)")
        logger.info("5. Map Y field to y_bin (range: 0 to 42.5)")
        logger.info("6. Create scatter plot with x_bin, y_bin")
        logger.info("7. Use Marks → Density or Size by total_shots for heatmap effect")
        logger.info("8. Filter by team_abbrev or shooter_name for specific views")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export shot location data for Tableau heatmaps"
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Include raw shot events (can be a large file)",
    )
    args = parser.parse_args()

    main(include_raw=args.include_raw)
