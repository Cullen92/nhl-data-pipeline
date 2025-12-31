{{
  config(
    materialized='view',
    tags=['bronze', 'raw']
  )
}}

-- Bronze layer: Raw game boxscore snapshots from S3
-- No transformations - immutable source of truth
-- Contains complete game data including player stats, team stats, scores

SELECT
    payload,
    s3_key,
    partition_date,
    game_id
FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }}
