"""
Bronze Layer: Player Prop Odds Snapshots

Reads raw odds JSON snapshots from S3 (raw/odds/player_props/...)
and writes them to Iceberg bronze layer with minimal transformation.

Schema:
- event_id: STRING (required) - Unique identifier for the odds event
- game_date: DATE (required) - Date of the game
- market: STRING (required) - Market type (e.g., "player_shots_on_goal")
- extracted_at: TIMESTAMP - When we fetched from the API
- source_url: STRING - API URL we fetched from
- payload: STRING - Full JSON payload as string
- partition_date: STRING - Date partition (YYYY-MM-DD)

Features:
- Partitioned by partition_date and market for query performance
- Incremental loading (skips already loaded events)
- Config loaded from config/iceberg.yml
- Detailed logging and error handling
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import boto3
import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, NestedField, StringType, TimestampType

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

def load_config(config_path: str = "config/iceberg.yml") -> dict:
    """Load Iceberg configuration from YAML file."""
    # Handle both absolute and relative paths
    if not Path(config_path).is_absolute():
        # Assume relative to project root
        project_root = Path(__file__).parent.parent
        config_path = project_root / config_path

    with open(config_path) as f:
        config = yaml.safe_load(f)

    logger.info(f"Loaded config from {config_path}")
    return config


# =============================================================================
# CATALOG SETUP
# =============================================================================

def get_catalog(config: dict):
    """Get the Glue catalog for Iceberg tables."""
    catalog_config = config["catalog"]

    return load_catalog(
        "glue",
        type=catalog_config["type"],
        **{"glue.region": catalog_config["region"]},
        warehouse=catalog_config["warehouse"],
    )


def ensure_namespace(catalog, namespace: str = "bronze"):
    """Create namespace if it doesn't exist."""
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Created namespace: '{namespace}'")
    except Exception:
        logger.debug(f"Namespace '{namespace}' already exists")


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

# Iceberg schema for bronze odds player props
ODDS_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "game_date", DateType(), required=True),
    NestedField(3, "market", StringType(), required=True),
    NestedField(4, "extracted_at", TimestampType(), required=True),
    NestedField(5, "source_url", StringType()),
    NestedField(6, "payload", StringType()),  # Raw JSON as string
    NestedField(7, "partition_date", StringType(), required=True),
)

# Partition specification: partition by partition_date and market
# This allows efficient queries like "get all player_shots_on_goal props for 2024-01-15"
ODDS_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=7,  # Field ID for partition_date
        field_id=1000,
        transform=IdentityTransform(),
        name="partition_date"
    ),
    PartitionField(
        source_id=3,  # Field ID for market
        field_id=1001,
        transform=IdentityTransform(),
        name="market"
    )
)

# Matching PyArrow schema (for writing)
ODDS_ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("game_date", pa.date32(), nullable=False),
    pa.field("market", pa.string(), nullable=False),
    pa.field("extracted_at", pa.timestamp("us"), nullable=False),
    pa.field("source_url", pa.string()),
    pa.field("payload", pa.string()),
    pa.field("partition_date", pa.string(), nullable=False),
])


def get_or_create_table(catalog, table_name: str = "bronze.odds_player_props"):
    """Get existing table or create it with proper partitioning."""
    try:
        table = catalog.load_table(table_name)
        logger.info(f"Loaded existing table: '{table_name}'")
        return table
    except NoSuchTableError:
        logger.info(f"Creating new table: '{table_name}'")
        table = catalog.create_table(
            table_name,
            schema=ODDS_SCHEMA,
            partition_spec=ODDS_PARTITION_SPEC,
            properties={
                "write.parquet.compression-codec": "snappy",
                "commit.retry.num-retries": "3",
            }
        )
        logger.info(f"Created Iceberg table: '{table_name}' with partition_date + market partitioning")
        return table


# =============================================================================
# S3 DATA LOADING
# =============================================================================

def list_raw_odds_files(
    bucket: str,
    prefix: str = "raw/odds/player_props/",
    region: str = "us-east-2"
) -> list[str]:
    """List all raw odds JSON files in S3."""
    s3 = boto3.client("s3", region_name=region)
    keys = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])

    logger.info(f"Found {len(keys)} raw odds files in S3")
    return keys


def read_raw_json_from_s3(bucket: str, key: str, region: str = "us-east-2") -> dict:
    """Read a single JSON file from S3."""
    s3 = boto3.client("s3", region_name=region)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        logger.error(f"S3 key not found: {key}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {key}: {e}")
        raise


def parse_snapshot_to_row(snapshot: dict, s3_key: str) -> dict:
    """
    Convert a raw odds snapshot dict to a bronze table row.

    Input (from your ingestion):
    {
        "extracted_at": "2024-10-08T12:00:00Z",
        "source_url": "https://api.the-odds-api.com/...",
        "event_id": "abc123xyz",
        "game_date": "2024-10-08",
        "market": "player_shots_on_goal",
        "payload": { ... full API response ... }
    }

    Output (for Iceberg):
    {
        "event_id": "abc123xyz",
        "game_date": date(2024, 10, 8),
        "market": "player_shots_on_goal",
        "extracted_at": datetime(...),
        "source_url": "...",
        "payload": '{"id": "abc123", ...}',  # JSON string
        "partition_date": "2024-10-08"
    }
    """
    # Parse extracted_at string to datetime
    extracted_at_str = snapshot.get("extracted_at", "")
    extracted_at = datetime.fromisoformat(extracted_at_str.replace("Z", "+00:00"))

    # Parse game_date string to date object
    game_date_str = snapshot.get("game_date", "")
    game_date = date.fromisoformat(game_date_str) if game_date_str else None

    # Extract partition_date from the S3 key or use game_date
    partition_date = extract_date_from_key(s3_key) or game_date_str

    return {
        "event_id": snapshot["event_id"],
        "game_date": game_date,
        "market": snapshot.get("market", "unknown"),
        "extracted_at": extracted_at,
        "source_url": snapshot.get("source_url", ""),
        "payload": json.dumps(snapshot.get("payload", {})),
        "partition_date": partition_date,
    }


def extract_date_from_key(s3_key: str) -> str:
    """Extract date from S3 key like 'raw/odds/.../date=2024-10-08/...'"""
    for part in s3_key.split("/"):
        if part.startswith("date="):
            return part.replace("date=", "")
    return ""


# =============================================================================
# INCREMENTAL LOADING
# =============================================================================

def get_existing_event_ids(table) -> set[str]:
    """
    Query the Iceberg table to get all event_ids already loaded.
    This enables incremental loading - we skip events we've already processed.
    """
    try:
        # Scan the table and collect event_ids
        scan = table.scan(selected_fields=("event_id",))

        existing_ids = set()
        for task in scan.plan_files():
            # Read the data file
            arrow_table = task.file.to_arrow()
            existing_ids.update(arrow_table["event_id"].to_pylist())

        logger.info(f"Found {len(existing_ids)} existing event_ids in table")
        return existing_ids
    except Exception as e:
        logger.warning(f"Could not read existing event_ids: {e}")
        return set()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def load_odds_to_iceberg(
    config: dict,
    limit: Optional[int] = None,
    skip_existing: bool = True
):
    """
    Main function: Read raw odds from S3 and write to Iceberg.

    Args:
        config: Configuration dictionary from iceberg.yml
        limit: Optional limit on number of files to process (for testing)
        skip_existing: If True, skip events already in the table (incremental mode)
    """
    # Setup
    catalog = get_catalog(config)
    ensure_namespace(catalog, "bronze")
    table = get_or_create_table(catalog)

    # Get existing event_ids for incremental loading
    existing_event_ids = get_existing_event_ids(table) if skip_existing else set()

    # List raw files
    bucket = config["aws"]["bucket"]
    region = config["aws"]["region"]
    raw_data_path = config["raw_data"]["odds_player_props"]
    prefix = raw_data_path.replace(f"s3://{bucket}/", "")

    keys = list_raw_odds_files(bucket, prefix, region)
    if limit:
        logger.info(f"Limiting to first {limit} files for testing")
        keys = keys[:limit]

    # Read and parse each file
    rows = []
    skipped = 0
    errors = 0

    for i, key in enumerate(keys):
        try:
            snapshot = read_raw_json_from_s3(bucket, key, region)
            event_id = snapshot.get("event_id")

            # Skip if already loaded
            if skip_existing and event_id in existing_event_ids:
                skipped += 1
                continue

            row = parse_snapshot_to_row(snapshot, key)
            rows.append(row)

            # Log progress every 100 files
            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(keys)} files...")

        except (KeyError, ValueError) as e:
            logger.error(f"Data error in {key}: {e}")
            errors += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {key}: {e}")
            errors += 1

    logger.info(f"Parsed {len(rows)} new snapshots, skipped {skipped}, errors {errors}")

    if not rows:
        logger.warning("No new data to load")
        return 0

    # Convert to PyArrow table
    try:
        arrow_table = pa.table({
            "event_id": [r["event_id"] for r in rows],
            "game_date": pa.array([r["game_date"] for r in rows], type=pa.date32()),
            "market": [r["market"] for r in rows],
            "extracted_at": pa.array([r["extracted_at"] for r in rows], type=pa.timestamp("us")),
            "source_url": [r["source_url"] for r in rows],
            "payload": [r["payload"] for r in rows],
            "partition_date": [r["partition_date"] for r in rows],
        }, schema=ODDS_ARROW_SCHEMA)
    except pa.ArrowInvalid as e:
        logger.error(f"Failed to create Arrow table: {e}")
        raise

    # Write to Iceberg
    try:
        table.append(arrow_table)
        logger.info(f"Successfully wrote {len(rows)} rows to bronze.odds_player_props")
    except Exception as e:
        logger.error(f"Failed to write to Iceberg: {e}")
        raise

    return len(rows)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load player prop odds to Iceberg bronze layer")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files to process")
    parser.add_argument("--config", type=str, default="config/iceberg.yml", help="Path to config file")
    parser.add_argument("--no-skip-existing", action="store_true", help="Disable incremental loading")

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Run
    rows_loaded = load_odds_to_iceberg(
        config=config,
        limit=args.limit,
        skip_existing=not args.no_skip_existing
    )

    logger.info(f"Done! Loaded {rows_loaded} new odds records")
