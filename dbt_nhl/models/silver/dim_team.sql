{{
  config(
    materialized='table'
  )
}}

-- Silver layer: Team dimension (sparse dimension)
-- One row per NHL team
-- Teams are relatively static, but we may get new teams (expansion) or relocations

WITH team_data AS (
    -- Get both home and away teams from boxscore snapshots
    SELECT DISTINCT
        payload:homeTeam.id::INT AS team_id,
        payload:homeTeam.abbrev::STRING AS team_abbrev,
        payload:homeTeam.placeName.default::STRING AS place_name,
        payload:homeTeam.commonName.default::STRING AS common_name
    FROM {{ ref('bronze_game_boxscore_snapshots') }}
    WHERE payload:homeTeam.id IS NOT NULL
    
    UNION
    
    SELECT DISTINCT
        payload:awayTeam.id::INT AS team_id,
        payload:awayTeam.abbrev::STRING AS team_abbrev,
        payload:awayTeam.placeName.default::STRING AS place_name,
        payload:awayTeam.commonName.default::STRING AS common_name
    FROM {{ ref('bronze_game_boxscore_snapshots') }}
    WHERE payload:awayTeam.id IS NOT NULL
)

SELECT
    team_id,
    team_abbrev,
    place_name,
    common_name,
    -- Construct full team name: "Toronto Maple Leafs"
    CONCAT(place_name, ' ', common_name) AS team_name,
    -- TODO: Add conference and division once we have that data source
    NULL AS conference,
    NULL AS division
FROM team_data
WHERE team_id IS NOT NULL
ORDER BY team_id
