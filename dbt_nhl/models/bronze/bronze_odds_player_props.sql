{{
  config(
    materialized='view',
    tags=['bronze', 'raw', 'odds']
  )
}}

-- Bronze layer: Raw player prop odds snapshots from S3
-- No transformations - immutable source of truth
-- Contains betting lines for player props (SOG, goals, assists, etc.)

SELECT
    payload,
    s3_key,
    partition_date as game_date,
    event_id,
    market
FROM {{ source('raw_odds', 'player_props') }}
