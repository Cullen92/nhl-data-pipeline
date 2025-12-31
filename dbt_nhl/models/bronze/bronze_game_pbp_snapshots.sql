{{
  config(
    materialized='view',
    tags=['bronze', 'raw']
  )
}}

-- Bronze layer: Raw play-by-play snapshots from S3
-- No transformations - immutable source of truth
-- Contains all game events: goals, shots, penalties, faceoffs, hits
-- Future use: shot location analysis, heat maps, event sequences

SELECT
    payload,
    s3_key,
    partition_date,
    game_id
FROM {{ source('raw_nhl', 'game_pbp_snapshots') }}
