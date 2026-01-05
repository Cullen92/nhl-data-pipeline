{{
  config(
    materialized='table',
    tags=['silver', 'bruins']
  )
}}

-- Bruins-only player shot locations for Tableau Public (Google Sheets size limit)
SELECT *
FROM {{ ref('player_shot_locations') }}
WHERE team_abbrev = 'BOS'
