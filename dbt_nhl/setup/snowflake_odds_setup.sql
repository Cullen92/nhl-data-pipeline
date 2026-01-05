-- Snowflake setup for Odds API data
-- Run this in Snowflake to create the required objects

-- 1. Create schema for odds data
CREATE SCHEMA IF NOT EXISTS NHL.RAW_ODDS;

-- 2. Create stage pointing to odds S3 path
-- Note: Uses the same storage integration as the NHL data
CREATE OR REPLACE STAGE NHL.RAW_ODDS.ODDS_S3_STAGE
    STORAGE_INTEGRATION = S3_NHL_INTEGRATION  -- Update if your integration has a different name
    URL = 's3://{{ env_var("ODDS_S3_BUCKET") }}/raw/odds/'
    FILE_FORMAT = (TYPE = JSON);

-- 3. Create table for player props
CREATE TABLE IF NOT EXISTS NHL.RAW_ODDS.PLAYER_PROPS (
    payload VARIANT,
    s3_key VARCHAR,
    partition_date DATE,
    event_id VARCHAR,
    market VARCHAR,
    ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Initial load of player props data
-- This loads all existing files from S3
COPY INTO NHL.RAW_ODDS.PLAYER_PROPS (payload, s3_key, partition_date, event_id, market)
FROM (
    SELECT 
        $1 AS payload,
        METADATA$FILENAME AS s3_key,
        -- Extract date from path: .../date=2024-10-10/...
        TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e')) AS partition_date,
        -- Extract event_id from filename: event_abc123.json
        REGEXP_SUBSTR(METADATA$FILENAME, 'event_([a-f0-9]+)\\.json', 1, 1, 'e') AS event_id,
        -- Extract market from path: .../market=player_shots_on_goal/...
        REGEXP_SUBSTR(METADATA$FILENAME, 'market=([^/]+)', 1, 1, 'e') AS market
    FROM @NHL.RAW_ODDS.ODDS_S3_STAGE/player_props/
)
PATTERN = '.*\\.json$'
ON_ERROR = 'CONTINUE';

-- 5. Verify load
SELECT 
    market,
    MIN(partition_date) AS earliest_date,
    MAX(partition_date) AS latest_date,
    COUNT(*) AS total_files
FROM NHL.RAW_ODDS.PLAYER_PROPS
GROUP BY market
ORDER BY market;
