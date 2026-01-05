{{
  config(
    materialized='table',
    tags=['silver', 'bruins']
  )
}}

-- Bruins-only team shot locations for Tableau Public (Google Sheets size limit)
-- Includes both offensive shots (by Bruins) and defensive shots (against Bruins)
SELECT *
FROM {{ ref('team_shot_locations') }}
WHERE team_abbrev = 'BOS'
