"""
Bronze Layer: Game Boxscore Snapshots

Reads raw JSON snapshots from S3 (raw/nhl/game_boxscore/...)
and writes them to Iceberg bronze layer with minimal transformation.

Schema:
- game_id: INT (required) - NHL game ID
- extracted_at: TIMESTAMP - When we fetched from the API
- source_url: STRING - API URL we fetched from
- payload: STRING - Full JSON payload as string
- partition_date: STRING - Date partition (YYYY-MM-DD)
"""
from __future__ import annotations

import json
from datetime import datetime

import boto3
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, TimestampType


# =============================================================================
# CATALOG SETUP
# =============================================================================

def get_catalog():
    """Get the Glue catalog for Iceberg tables."""
    return load_catalog(
        "glue",
        type="glue",
        **{"glue.region": "us-east-2"},
        warehouse="s3://nhl-data-pipeline-cullenm-dev/iceberg",
    )


def ensure_namespace(catalog, namespace: str = "bronze"):
    """Create namespace if it doesn't exist."""
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: '{namespace}'")
    except Exception:
        pass  # Already exists


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

# Iceberg schema for bronze game boxscore
BOXSCORE_SCHEMA = Schema(
    NestedField(1, "game_id", IntegerType(), required=True),
    NestedField(2, "extracted_at", TimestampType(), required=True),
    NestedField(3, "source_url", StringType()),
    NestedField(4, "payload", StringType()),  # Raw JSON as string
    NestedField(5, "partition_date", StringType(), required=True),
)

# Matching PyArrow schema (for writing)
BOXSCORE_ARROW_SCHEMA = pa.schema([
    pa.field("game_id", pa.int32(), nullable=False),
    pa.field("extracted_at", pa.timestamp("us"), nullable=False),
    pa.field("source_url", pa.string()),
    pa.field("payload", pa.string()),
    pa.field("partition_date", pa.string(), nullable=False),
])


def get_or_create_table(catalog, table_name: str = "bronze.game_boxscore"):
    """Get existing table or create it."""
    try:
        table = catalog.create_table(table_name, schema=BOXSCORE_SCHEMA)
        print(f"Created Iceberg table: '{table_name}'")
        return table
    except Exception:
        print(f"Loading existing table: '{table_name}'")
        return catalog.load_table(table_name)


# =============================================================================
# S3 DATA LOADING
# =============================================================================

def list_raw_boxscore_files(bucket: str, prefix: str = "raw/nhl/game_boxscore/") -> list[str]:
    """List all raw boxscore JSON files in S3."""
    s3 = boto3.client("s3", region_name="us-east-2")
    keys = []
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    
    print(f"Found {len(keys)} raw boxscore files in S3")
    return keys


def read_raw_json_from_s3(bucket: str, key: str) -> dict:
    """Read a single JSON file from S3."""
    s3 = boto3.client("s3", region_name="us-east-2")
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


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
    """Extract date from S3 key like 'raw/nhl/.../date=2024-10-08/...'"""
    for part in s3_key.split("/"):
        if part.startswith("date="):
            return part.replace("date=", "")
    return "unknown"


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def load_boxscores_to_iceberg(bucket: str, limit: int | None = None):
    """
    Main function: Read raw boxscores from S3 and write to Iceberg.
    
    Args:
        bucket: S3 bucket name
        limit: Optional limit on number of files to process (for testing)
    """
    # Setup
    catalog = get_catalog()
    ensure_namespace(catalog, "bronze")
    table = get_or_create_table(catalog)
    
    # List raw files
    keys = list_raw_boxscore_files(bucket)
    if limit:
        keys = keys[:limit]
    
    # Read and parse each file
    rows = []
    for key in keys:
        try:
            snapshot = read_raw_json_from_s3(bucket, key)
            row = parse_snapshot_to_row(snapshot, key)
            rows.append(row)
        except Exception as e:
            print(f"Error processing {key}: {e}")
    
    print(f"Parsed {len(rows)} snapshots")
    
    # Convert to PyArrow table
    arrow_table = pa.table({
        "game_id": pa.array([r["game_id"] for r in rows], type=pa.int32()),
        "extracted_at": pa.array([r["extracted_at"] for r in rows], type=pa.timestamp("us")),
        "source_url": [r["source_url"] for r in rows],
        "payload": [r["payload"] for r in rows],
        "partition_date": [r["partition_date"] for r in rows],
    }, schema=BOXSCORE_ARROW_SCHEMA)
    
    # Write to Iceberg
    table.append(arrow_table)
    print(f"Wrote {len(rows)} rows to bronze.game_boxscore")
    
    return len(rows)


if __name__ == "__main__":
    # Run with a small limit first to test
    BUCKET = "nhl-data-pipeline-cullenm-dev"
    load_boxscores_to_iceberg(BUCKET, limit=5)