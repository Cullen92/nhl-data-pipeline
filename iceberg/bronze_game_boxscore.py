"""
Bronze Layer: Game Boxscore Snapshots

Reads raw JSON snapshots from S3 (raw/nhl/game_boxscore/...)
and writes them to Iceberg bronze layer with minimal transformation.

Schema:
- game_id: LONG (required) - NHL game ID (10 digits, needs int64)
- extracted_at: TIMESTAMP - When we fetched from the API
- source_url: STRING - API URL we fetched from
- payload: STRING - Full JSON payload as string
- partition_date: STRING - Date partition (YYYY-MM-DD)

Improvements from POC:
- Uses int64 for game_id (NHL IDs are 10 digits)
- Partitioned by partition_date for query performance
- Incremental loading (skips already loaded games)
- Config loaded from config/iceberg.yml
- Detailed logging and error handling
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
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
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

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

# Iceberg schema for bronze game boxscore
# NOTE: Using LongType (int64) instead of IntegerType (int32) for game_id
# NHL game IDs are 10 digits (e.g., 2024020001), which can overflow int32
BOXSCORE_SCHEMA = Schema(
    NestedField(1, "game_id", LongType(), required=True),
    NestedField(2, "extracted_at", TimestampType(), required=True),
    NestedField(3, "source_url", StringType()),
    NestedField(4, "payload", StringType()),  # Raw JSON as string
    NestedField(5, "partition_date", StringType(), required=True),
)

# Partition specification: partition by partition_date
BOXSCORE_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=5,  # Field ID for partition_date
        field_id=1000,
        transform=IdentityTransform(),
        name="partition_date"
    )
)

# Matching PyArrow schema (for writing)
BOXSCORE_ARROW_SCHEMA = pa.schema([
    pa.field("game_id", pa.int64(), nullable=False),
    pa.field("extracted_at", pa.timestamp("us"), nullable=False),
    pa.field("source_url", pa.string()),
    pa.field("payload", pa.string()),
    pa.field("partition_date", pa.string(), nullable=False),
])


def get_or_create_table(catalog, table_name: str = "bronze.game_boxscore"):
    """Get existing table or create it with proper partitioning."""
    try:
        table = catalog.load_table(table_name)
        logger.info(f"Loaded existing table: '{table_name}'")
        return table
    except NoSuchTableError:
        logger.info(f"Creating new table: '{table_name}'")
        table = catalog.create_table(
            table_name,
            schema=BOXSCORE_SCHEMA,
            partition_spec=BOXSCORE_PARTITION_SPEC,
            properties={
                "write.parquet.compression-codec": "snappy",
                "commit.retry.num-retries": "3",
            }
        )
        logger.info(f"Created Iceberg table: '{table_name}' with partition_date partitioning")
        return table


# =============================================================================
# S3 DATA LOADING
# =============================================================================

def list_raw_boxscore_files(
    bucket: str,
    prefix: str = "raw/nhl/game_boxscore/",
    region: str = "us-east-2"
) -> list[str]:
    """List all raw boxscore JSON files in S3."""
    s3 = boto3.client("s3", region_name=region)
    keys = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])

    logger.info(f"Found {len(keys)} raw boxscore files in S3")
    return keys


def read_raw_json_from_s3(s3_client, bucket: str, key: str) -> dict:
    """
    Read a single JSON file from S3.

    Args:
        s3_client: Reused boto3 S3 client (for performance in loops)
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        Parsed JSON data as dict
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"S3 key not found: {key}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {key}: {e}")
        raise


def parse_snapshot_to_row(snapshot: dict, s3_key: str) -> dict:
    """
    Convert a raw snapshot dict to a bronze table row.

    Input (from your ingestion):
    {
        "extracted_at": "2024-10-08T12:00:00Z",
        "source_url": "https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore",
        "game_id": 2024020001,
        "payload": { ... full API response ... }
    }

    Output (for Iceberg):
    {
        "game_id": 2024020001,
        "extracted_at": datetime(...),
        "source_url": "...",
        "payload": '{"id": 2024020001, ...}',  # JSON string
        "partition_date": "2024-10-08"
    }
    """
    # Parse extracted_at string to datetime
    extracted_at_str = snapshot.get("extracted_at", "")
    extracted_at = datetime.fromisoformat(extracted_at_str.replace("Z", "+00:00"))

    # Extract partition_date from the S3 key (e.g., "raw/nhl/.../date=2024-10-08/...")
    partition_date = extract_date_from_key(s3_key)

    return {
        "game_id": snapshot["game_id"],
        "extracted_at": extracted_at,
        "source_url": snapshot.get("source_url", ""),
        "payload": json.dumps(snapshot.get("payload", {})),
        "partition_date": partition_date,
    }


def extract_date_from_key(s3_key: str) -> str:
    """
    Extract date from S3 key like 'raw/nhl/.../date=2024-10-08/...'.

    Raises:
        ValueError: If date= segment is missing from the S3 key
    """
    for part in s3_key.split("/"):
        if part.startswith("date="):
            return part.replace("date=", "")

    # Fail fast on malformed S3 keys instead of writing garbage partitions
    error_msg = f"Could not extract date from S3 key (missing 'date=' segment): {s3_key}"
    logger.error(error_msg)
    raise ValueError(error_msg)


# =============================================================================
# INCREMENTAL LOADING
# =============================================================================

def get_existing_game_ids(table) -> set[int]:
    """
    Query the Iceberg table to get all game_ids already loaded.
    This enables incremental loading - we skip games we've already processed.
    """
    try:
        # Scan the table and collect game_ids
        scan = table.scan(selected_fields=("game_id",))

        # Use the scan API to read directly into an Arrow table
        arrow_table = scan.to_arrow()
        existing_ids = set(arrow_table["game_id"].to_pylist())

        logger.info(f"Found {len(existing_ids)} existing game_ids in table")
        return existing_ids
    except Exception as e:
        logger.warning(f"Could not read existing game_ids: {e}")
        return set()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def load_boxscores_to_iceberg(
    config: dict,
    limit: Optional[int] = None,
    skip_existing: bool = True
):
    """
    Main function: Read raw boxscores from S3 and write to Iceberg.

    Args:
        config: Configuration dictionary from iceberg.yml
        limit: Optional limit on number of files to process (for testing)
        skip_existing: If True, skip games already in the table (incremental mode)
    """
    # Setup
    catalog = get_catalog(config)
    ensure_namespace(catalog, "bronze")
    table = get_or_create_table(catalog)

    # Get existing game_ids for incremental loading
    existing_game_ids = get_existing_game_ids(table) if skip_existing else set()

    # List raw files
    bucket = config["aws"]["bucket"]
    region = config["aws"]["region"]
    raw_data_path = config["raw_data"]["game_boxscore"]
    prefix = raw_data_path.replace(f"s3://{bucket}/", "")

    keys = list_raw_boxscore_files(bucket, prefix, region)
    if limit:
        logger.info(f"Limiting to first {limit} files for testing")
        keys = keys[:limit]

    # Create S3 client once for reuse across all files (performance optimization)
    s3_client = boto3.client("s3", region_name=region)

    # Read and parse each file
    rows = []
    skipped = 0
    errors = 0

    for i, key in enumerate(keys):
        try:
            snapshot = read_raw_json_from_s3(s3_client, bucket, key)
            game_id = snapshot.get("game_id")

            # Skip if already loaded
            if skip_existing and game_id in existing_game_ids:
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
            "game_id": pa.array([r["game_id"] for r in rows], type=pa.int64()),
            "extracted_at": pa.array([r["extracted_at"] for r in rows], type=pa.timestamp("us")),
            "source_url": [r["source_url"] for r in rows],
            "payload": [r["payload"] for r in rows],
            "partition_date": [r["partition_date"] for r in rows],
        }, schema=BOXSCORE_ARROW_SCHEMA)
    except pa.ArrowInvalid as e:
        logger.error(f"Failed to create Arrow table: {e}")
        raise

    # Write to Iceberg
    try:
        table.append(arrow_table)
        logger.info(f"Successfully wrote {len(rows)} rows to bronze.game_boxscore")
    except Exception as e:
        logger.error(f"Failed to write to Iceberg: {e}")
        raise

    return len(rows)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load NHL boxscores to Iceberg bronze layer")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files to process")
    parser.add_argument("--config", type=str, default="config/iceberg.yml", help="Path to config file")
    parser.add_argument("--no-skip-existing", action="store_true", help="Disable incremental loading")

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Run
    rows_loaded = load_boxscores_to_iceberg(
        config=config,
        limit=args.limit,
        skip_existing=not args.no_skip_existing
    )

    logger.info(f"Done! Loaded {rows_loaded} new game boxscores")
