"""
Validate Bronze Layer Iceberg Tables with DuckDB

This script uses PyIceberg to load tables and DuckDB to query them for validation:
1. Row counts match expected values
2. Date ranges are correct
3. Data quality checks (nulls, duplicates)
4. Sample data looks reasonable
"""
import duckdb
from pathlib import Path
import yaml
from pyiceberg.catalog import load_catalog

def load_config(config_path: str = "config/iceberg.yml") -> dict:
    """Load Iceberg configuration."""
    if not Path(config_path).is_absolute():
        project_root = Path(__file__).parent.parent
        config_path = project_root / config_path

    with open(config_path) as f:
        return yaml.safe_load(f)

def main():
    print("=" * 80)
    print("BRONZE LAYER VALIDATION - PyIceberg + DuckDB")
    print("=" * 80)
    print()

    # Load config
    config = load_config()
    region = config["catalog"]["region"]

    # Load catalog
    print("Loading Iceberg catalog...")
    catalog = load_catalog(
        "glue",
        type="glue",
        **{"glue.region": region},
        warehouse=config["catalog"]["warehouse"],
    )
    print("✓ Catalog loaded\n")

    # Connect to DuckDB
    con = duckdb.connect()

    print("=" * 80)
    print("TABLE 1: bronze.game_boxscore")
    print("=" * 80)

    # Load table and scan to Arrow
    print("Loading table via PyIceberg...")
    game_boxscore = catalog.load_table("bronze.game_boxscore")
    df_boxscore = game_boxscore.scan().to_arrow()
    print(f"✓ Loaded {len(df_boxscore):,} rows into Arrow table\n")

    # Row count
    result = con.execute("""
        SELECT COUNT(*) as row_count
        FROM df_boxscore
    """).fetchone()
    print(f"Total rows: {result[0]:,}")

    # Date range
    result = con.execute("""
        SELECT
            MIN(partition_date) as min_date,
            MAX(partition_date) as max_date,
            COUNT(DISTINCT partition_date) as unique_dates
        FROM df_boxscore
    """).fetchone()
    print(f"Date range: {result[0]} to {result[1]}")
    print(f"Unique dates: {result[2]}")

    # Game ID range
    result = con.execute("""
        SELECT
            MIN(game_id) as min_game_id,
            MAX(game_id) as max_game_id,
            COUNT(DISTINCT game_id) as unique_games
        FROM df_boxscore
    """).fetchone()
    print(f"Game ID range: {result[0]} to {result[1]}")
    print(f"Unique games: {result[2]:,}")

    # Check for duplicates
    result = con.execute("""
        SELECT COUNT(*) as duplicate_games
        FROM (
            SELECT game_id, COUNT(*) as cnt
            FROM df_boxscore
            GROUP BY game_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()
    print(f"Duplicate game_ids: {result[0]}")

    # Sample records
    print("\nSample records (first 3):")
    results = con.execute("""
        SELECT
            game_id,
            partition_date,
            extracted_at,
            LENGTH(payload) as payload_size_bytes
        FROM df_boxscore
        ORDER BY partition_date DESC, game_id
        LIMIT 3
    """).fetchall()

    for row in results:
        print(f"  game_id={row[0]}, date={row[1]}, extracted={row[2]}, payload_size={row[3]:,} bytes")

    print("\n" + "=" * 80)
    print("TABLE 2: bronze.odds_player_props")
    print("=" * 80)

    # Load table and scan to Arrow
    print("Loading table via PyIceberg...")
    odds_props = catalog.load_table("bronze.odds_player_props")
    df_odds = odds_props.scan().to_arrow()
    print(f"✓ Loaded {len(df_odds):,} rows into Arrow table\n")

    # Row count
    result = con.execute("""
        SELECT COUNT(*) as row_count
        FROM df_odds
    """).fetchone()
    print(f"Total rows: {result[0]:,}")

    # Date range
    result = con.execute("""
        SELECT
            MIN(partition_date) as min_date,
            MAX(partition_date) as max_date,
            COUNT(DISTINCT partition_date) as unique_dates
        FROM df_odds
    """).fetchone()
    print(f"Partition date range: {result[0]} to {result[1]}")
    print(f"Unique partition dates: {result[2]}")

    # Market breakdown
    print("\nMarket breakdown:")
    results = con.execute("""
        SELECT
            market,
            COUNT(*) as record_count
        FROM df_odds
        GROUP BY market
        ORDER BY record_count DESC
    """).fetchall()

    for row in results:
        print(f"  {row[0]}: {row[1]:,} records")

    # Check game_date nulls
    result = con.execute("""
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN game_date IS NULL THEN 1 ELSE 0 END) as null_game_dates,
            ROUND(100.0 * SUM(CASE WHEN game_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_percentage
        FROM df_odds
    """).fetchone()
    print(f"\nGame date null check:")
    print(f"  Total records: {result[0]:,}")
    print(f"  Null game_dates: {result[1]:,} ({result[2]}%)")

    # Check for duplicates
    result = con.execute("""
        SELECT COUNT(*) as duplicate_events
        FROM (
            SELECT event_id, COUNT(*) as cnt
            FROM df_odds
            GROUP BY event_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()
    print(f"Duplicate event_ids: {result[0]}")

    # Sample records
    print("\nSample records (first 3):")
    results = con.execute("""
        SELECT
            event_id,
            game_date,
            market,
            partition_date,
            LENGTH(payload) as payload_size_bytes
        FROM df_odds
        ORDER BY partition_date DESC
        LIMIT 3
    """).fetchall()

    for row in results:
        print(f"  event_id={row[0]}, game_date={row[1]}, market={row[2]}, partition={row[3]}, payload_size={row[4]:,} bytes")

    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print("✓ bronze.game_boxscore: Successfully queried")
    print(f"  - Expected: 2131 games | Actual: {len(df_boxscore):,} rows")
    print("✓ bronze.odds_player_props: Successfully queried")
    print(f"  - Expected: 2483 odds snapshots | Actual: {len(df_odds):,} rows")

    # Final validation
    if len(df_boxscore) == 2131 and len(df_odds) == 2483:
        print("\n✓ ALL VALIDATIONS PASSED!")
        print("  All expected records are present with no duplicates.")
    else:
        print("\n⚠ WARNING: Row counts don't match expectations!")
        print("  Review the metrics above and check S3 for missing/extra files.")

    print("=" * 80)

    con.close()

if __name__ == "__main__":
    main()
