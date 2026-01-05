{{
    config(
        materialized='table',
        tags=['silver', 'bruins', 'tableau']
    )
}}

-- Bruins-only shot events for Tableau Public (Google Sheets size limit)
-- Includes shots by Bruins players and shots against the Bruins
SELECT *
FROM {{ ref('fact_shot_events') }}
WHERE home_team_abbrev = 'BOS' OR away_team_abbrev = 'BOS'
