"""
Silver Layer: Player Game Statistics (PySpark)

Reads bronze.game_boxscore Iceberg table, parses JSON payloads, and writes
a clean fact table with one row per player per game.

Replicates the dbt fact_player_game_stats model using PySpark transformations.

Key transformations:
1. Parse JSON payload into a typed struct (single parse, used throughout)
2. Filter to completed games only (gameState = 'OFF')
3. Deduplicate: most recent snapshot per game_id
4. Explode player arrays (home/away x forwards/defense)
5. Extract player statistics from struct fields
6. Build dimension tables (dim_player, dim_team)
7. Write to silver Iceberg tables

Usage:
    python spark/silver_player_game_stats.py
    python spark/silver_player_game_stats.py --limit 50  # Test with subset
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from spark.setup_spark import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

# Schema for individual player stats within the boxscore payload.
# This matches the NHL API response structure for playerByGameStats.
PLAYER_STATS_SCHEMA = StructType([
    StructField("playerId", LongType()),
    StructField("name", StructType([
        StructField("default", StringType()),
    ])),
    StructField("position", StringType()),
    StructField("goals", IntegerType()),
    StructField("assists", IntegerType()),
    StructField("points", IntegerType()),
    StructField("plusMinus", IntegerType()),
    StructField("sog", IntegerType()),
    StructField("pim", IntegerType()),
    StructField("powerPlayGoals", IntegerType()),
    StructField("shorthandedGoals", IntegerType()),
    StructField("hits", IntegerType()),
    StructField("blockedShots", IntegerType()),
    StructField("giveaways", IntegerType()),
    StructField("takeaways", IntegerType()),
    StructField("faceoffWinningPctg", DoubleType()),
    StructField("toi", StringType()),
    StructField("shifts", IntegerType()),
])

# Schema for a team's player stats (forwards + defense arrays)
TEAM_PLAYER_STATS_SCHEMA = StructType([
    StructField("forwards", ArrayType(PLAYER_STATS_SCHEMA)),
    StructField("defense", ArrayType(PLAYER_STATS_SCHEMA)),
])

# Schema for a team (id, abbrev, names, etc.)
TEAM_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("abbrev", StringType()),
    StructField("name", StructType([StructField("default", StringType())])),
    StructField("placeName", StructType([StructField("default", StringType())])),
    StructField("commonName", StructType([StructField("default", StringType())])),
    StructField("score", IntegerType()),
])

# Top-level boxscore payload schema (only the fields we need)
BOXSCORE_PAYLOAD_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("season", IntegerType()),
    StructField("gameType", IntegerType()),
    StructField("gameDate", StringType()),  # Parse to date after extraction
    StructField("gameState", StringType()),
    StructField("venue", StructType([StructField("default", StringType())])),
    StructField("homeTeam", TEAM_SCHEMA),
    StructField("awayTeam", TEAM_SCHEMA),
    StructField("playerByGameStats", StructType([
        StructField("homeTeam", TEAM_PLAYER_STATS_SCHEMA),
        StructField("awayTeam", TEAM_PLAYER_STATS_SCHEMA),
    ])),
])


# =============================================================================
# STEP 1: READ AND PARSE BRONZE DATA
# =============================================================================


def read_bronze_boxscores(spark: SparkSession) -> DataFrame:
    """Read bronze.game_boxscore and parse JSON payload into a typed struct.

    The payload is parsed ONCE into a struct column called 'data'.
    All downstream transformations reference data.* fields, never raw JSON.
    """
    logger.info("Reading bronze.game_boxscore...")
    bronze = spark.table("glue.bronze.game_boxscore")
    logger.info(f"Bronze rows: {bronze.count():,}")

    # Parse the entire payload into a typed struct - one parse, used everywhere
    parsed = bronze.withColumn("data", F.from_json(F.col("payload"), BOXSCORE_PAYLOAD_SCHEMA))

    # Flatten the top-level fields we need for filtering/joining
    parsed = (
        parsed
        .withColumn("game_state", F.col("data.gameState"))
        .withColumn("game_date", F.col("data.gameDate").cast(DateType()))
        .withColumn("season", F.col("data.season"))
        .withColumn("home_team_id", F.col("data.homeTeam.id"))
        .withColumn("home_team_abbrev", F.col("data.homeTeam.abbrev"))
        .withColumn("home_place_name", F.col("data.homeTeam.placeName.default"))
        .withColumn("home_common_name", F.col("data.homeTeam.commonName.default"))
        .withColumn("away_team_id", F.col("data.awayTeam.id"))
        .withColumn("away_team_abbrev", F.col("data.awayTeam.abbrev"))
        .withColumn("away_place_name", F.col("data.awayTeam.placeName.default"))
        .withColumn("away_common_name", F.col("data.awayTeam.commonName.default"))
    )

    return parsed


# =============================================================================
# STEP 2: FILTER AND DEDUPLICATE
# =============================================================================


def filter_completed_games(df: DataFrame) -> DataFrame:
    """Filter to completed games only (gameState = 'OFF')."""
    completed = df.filter(F.col("game_state") == "OFF")
    logger.info(f"Completed games (game_state=OFF): {completed.select('game_id').distinct().count():,}")
    return completed


def deduplicate_snapshots(df: DataFrame) -> DataFrame:
    """Keep only the most recent snapshot per game_id.

    Equivalent to dbt pattern:
    QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY partition_date DESC) = 1
    """
    window = Window.partitionBy("game_id").orderBy(
        F.col("partition_date").desc(),
        F.col("extracted_at").desc(),
    )
    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    logger.info(f"After deduplication: {deduped.count():,} unique games")
    return deduped


# =============================================================================
# STEP 3: EXTRACT PLAYER STATS
# =============================================================================


def extract_players_for_side(
    df: DataFrame,
    team_side: str,
    position_group: str,
    home_away: str,
    position_type: str,
) -> DataFrame:
    """Extract player stats for one team/position combination.

    Args:
        df: DataFrame with parsed 'data' struct column
        team_side: 'homeTeam' or 'awayTeam' (path into data.playerByGameStats)
        position_group: 'forwards' or 'defense'
        home_away: 'home' or 'away' (label for output)
        position_type: 'F' or 'D' (label for output)
    """
    # Access the player array from the parsed struct
    players_col = f"data.playerByGameStats.{team_side}.{position_group}"

    return (
        df
        .withColumn("players", F.col(players_col))
        .filter(F.col("players").isNotNull())
        .withColumn("player", F.explode("players"))
        .filter(F.col("player.playerId").isNotNull())
        .select(
            # Game context
            F.col("game_id"),
            F.col("game_date"),
            F.col("season"),
            # Player identity
            F.col("player.playerId").alias("player_id"),
            F.col("player.name.default").alias("player_name"),
            F.col("player.position").alias("position_code"),
            F.lit(position_type).alias("position_type"),
            # Team context
            F.lit(home_away).alias("home_away"),
            F.when(
                F.lit(home_away) == "home",
                F.col("home_team_id"),
            ).otherwise(F.col("away_team_id")).alias("team_id"),
            F.when(
                F.lit(home_away) == "home",
                F.col("home_team_abbrev"),
            ).otherwise(F.col("away_team_abbrev")).alias("team_abbrev"),
            F.when(
                F.lit(home_away) == "home",
                F.col("away_team_id"),
            ).otherwise(F.col("home_team_id")).alias("opponent_team_id"),
            F.when(
                F.lit(home_away) == "home",
                F.col("away_team_abbrev"),
            ).otherwise(F.col("home_team_abbrev")).alias("opponent_team_abbrev"),
            # Player stats
            F.col("player.goals").alias("goals"),
            F.col("player.assists").alias("assists"),
            F.col("player.points").alias("points"),
            F.col("player.plusMinus").alias("plus_minus"),
            F.col("player.sog").alias("shots"),
            F.col("player.pim").alias("penalty_minutes"),
            F.col("player.powerPlayGoals").alias("pp_goals"),
            F.col("player.shorthandedGoals").alias("sh_goals"),
            F.col("player.hits").alias("hits"),
            F.col("player.blockedShots").alias("blocked_shots"),
            F.col("player.giveaways").alias("giveaways"),
            F.col("player.takeaways").alias("takeaways"),
            F.col("player.faceoffWinningPctg").alias("faceoff_win_pct"),
            F.col("player.toi").alias("time_on_ice"),
            F.col("player.shifts").alias("shifts"),
        )
    )


def extract_player_stats(df: DataFrame) -> DataFrame:
    """Extract player statistics from all 4 team/position combinations.

    Unions home forwards + home defense + away forwards + away defense.
    """
    combinations = [
        ("homeTeam", "forwards", "home", "F"),
        ("homeTeam", "defense", "home", "D"),
        ("awayTeam", "forwards", "away", "F"),
        ("awayTeam", "defense", "away", "D"),
    ]

    player_dfs = [
        extract_players_for_side(df, team_side, pos_group, home_away, pos_type)
        for team_side, pos_group, home_away, pos_type in combinations
    ]

    # Union all 4 combinations
    result = player_dfs[0]
    for player_df in player_dfs[1:]:
        result = result.unionByName(player_df)

    logger.info(f"Extracted player-game rows: {result.count():,}")
    return result


# =============================================================================
# STEP 4: BUILD DIMENSION TABLES
# =============================================================================


def build_dim_team(df: DataFrame) -> DataFrame:
    """Build dim_team from deduplicated game data.

    Extracts unique teams from both home and away sides using
    the already-flattened columns (no re-parsing of payload).
    """
    home_teams = df.select(
        F.col("home_team_id").alias("team_id"),
        F.col("home_team_abbrev").alias("team_abbrev"),
        F.col("home_place_name").alias("place_name"),
        F.col("home_common_name").alias("common_name"),
    )

    away_teams = df.select(
        F.col("away_team_id").alias("team_id"),
        F.col("away_team_abbrev").alias("team_abbrev"),
        F.col("away_place_name").alias("place_name"),
        F.col("away_common_name").alias("common_name"),
    )

    all_teams = (
        home_teams.union(away_teams)
        .filter(F.col("team_id").isNotNull())
        .distinct()
    )

    # Deduplicate by team_id
    window = Window.partitionBy("team_id").orderBy("team_abbrev")
    dim_team = (
        all_teams
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("team_name", F.concat(F.col("place_name"), F.lit(" "), F.col("common_name")))
    )

    logger.info(f"dim_team: {dim_team.count()} teams")
    return dim_team


def build_dim_player(player_stats: DataFrame) -> DataFrame:
    """Build dim_player from player game stats.

    Takes the most recent game appearance to determine current team.
    """
    window = Window.partitionBy("player_id").orderBy(
        F.col("game_date").desc(),
        F.col("game_id").desc(),
    )

    dim_player = (
        player_stats
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "player_id",
            "player_name",
            "position_code",
            "position_type",
            F.col("team_id").alias("current_team_id"),
            F.col("team_abbrev").alias("current_team_abbrev"),
        )
    )

    logger.info(f"dim_player: {dim_player.count()} players")
    return dim_player


# =============================================================================
# STEP 5: WRITE TO ICEBERG
# =============================================================================


def ensure_namespace(spark: SparkSession, namespace: str):
    """Create namespace if it doesn't exist."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue.{namespace}")
        logger.info(f"Namespace '{namespace}' ready")
    except Exception as e:
        logger.debug(f"Namespace check: {e}")


def write_table(df: DataFrame, table_name: str, spark: SparkSession, partition_by: list[str] | None = None):
    """Write DataFrame to Iceberg table with create-or-replace semantics."""
    full_name = f"glue.{table_name}"

    # Check if table exists
    try:
        spark.table(full_name)
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists:
        df.writeTo(full_name).overwrite(F.lit(True))
        logger.info(f"Overwrote table: {full_name}")
    else:
        writer = df.writeTo(full_name)
        if partition_by:
            for col_name in partition_by:
                writer = writer.partitionedBy(col_name)
        writer.create()
        logger.info(f"Created table: {full_name}")


# =============================================================================
# MAIN PIPELINE
# =============================================================================


def run_silver_pipeline(spark: SparkSession, limit: int | None = None):
    """Execute the full silver layer pipeline."""
    logger.info("=" * 60)
    logger.info("SILVER LAYER: Player Game Statistics Pipeline")
    logger.info("=" * 60)

    # Step 1: Read and parse bronze data (single JSON parse)
    bronze_df = read_bronze_boxscores(spark)

    if limit:
        bronze_df = bronze_df.limit(limit)
        logger.info(f"Limited to {limit} rows for testing")

    # Step 2: Filter completed games and deduplicate
    completed = filter_completed_games(bronze_df)
    deduped = deduplicate_snapshots(completed)

    # Step 3: Extract player stats from parsed struct
    player_stats = extract_player_stats(deduped)

    # Step 4: Build dimensions from already-parsed data
    dim_team = build_dim_team(deduped)
    dim_player = build_dim_player(player_stats)

    # Step 5: Write to Iceberg
    ensure_namespace(spark, "silver")

    logger.info("Writing silver tables...")
    write_table(dim_team, "silver.dim_team", spark)
    write_table(dim_player, "silver.dim_player", spark)
    write_table(
        player_stats,
        "silver.fact_player_game_stats",
        spark,
        partition_by=["season"],
    )

    # Summary
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  dim_team:                {dim_team.count():,} rows")
    logger.info(f"  dim_player:              {dim_player.count():,} rows")
    logger.info(f"  fact_player_game_stats:  {player_stats.count():,} rows")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer: Player Game Stats")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit number of bronze rows to process (for testing)",
    )
    args = parser.parse_args()

    spark = get_spark_session("silver-player-game-stats")
    try:
        run_silver_pipeline(spark, limit=args.limit)
    finally:
        spark.stop()
