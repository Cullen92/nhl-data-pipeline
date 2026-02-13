"""
Spark Session Configuration with Iceberg + Glue Catalog

Creates a SparkSession configured to read/write Iceberg tables
via the AWS Glue Data Catalog. Used by all silver/gold layer jobs.

Usage:
    from spark.setup_spark import get_spark_session
    spark = get_spark_session("my-job-name")
    df = spark.table("glue.bronze.game_boxscore")
"""
from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/iceberg.yml") -> dict:
    """Load Iceberg configuration from YAML file."""
    if not Path(config_path).is_absolute():
        project_root = Path(__file__).parent.parent
        config_path = project_root / config_path

    with open(config_path) as f:
        config = yaml.safe_load(f)

    logger.info(f"Loaded config from {config_path}")
    return config


def get_spark_session(
    app_name: str = "nhl-iceberg",
    config_path: str = "config/iceberg.yml",
    local: bool = True,
) -> SparkSession:
    """Create a SparkSession configured for Iceberg + Glue Catalog.

    Args:
        app_name: Spark application name
        config_path: Path to iceberg.yml configuration
        local: If True, run in local mode (no cluster required)

    Returns:
        Configured SparkSession
    """
    config = load_config(config_path)
    catalog_config = config["catalog"]
    region = catalog_config["region"]
    warehouse = catalog_config["warehouse"]

    # Iceberg Spark runtime + AWS dependencies
    # Spark 4.0 uses Scala 2.13; Iceberg 1.10.1 has official Spark 4.0 support
    packages = ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
        "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
    ])

    master = "local[*]" if local else None

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", packages)
        # Iceberg catalog configuration
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue.warehouse", warehouse)
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.region", region)
        # Iceberg SQL extensions (MERGE INTO, etc.)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Default catalog for unqualified table names
        .config("spark.sql.defaultCatalog", "glue")
        # Performance settings
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if master:
        builder = builder.master(master)

    spark = builder.getOrCreate()
    logger.info(
        f"SparkSession created: app={app_name}, master={spark.sparkContext.master}, "
        f"catalog=glue ({warehouse})"
    )

    return spark


if __name__ == "__main__":
    # Quick smoke test: create session and list tables
    spark = get_spark_session("setup-test")

    print("\n--- Iceberg Catalog Test ---")
    print("Listing bronze tables...")
    spark.sql("SHOW TABLES IN glue.bronze").show()

    print("Reading bronze.game_boxscore (limit 3)...")
    spark.table("glue.bronze.game_boxscore").select(
        "game_id", "partition_date", "extracted_at"
    ).orderBy("partition_date").limit(3).show()

    spark.stop()
    print("--- Test Complete ---")
