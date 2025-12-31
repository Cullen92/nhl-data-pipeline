{{
  config(
    materialized='view'
  )
}}

-- Extract clean game information from raw boxscore snapshots
-- Takes the most recent snapshot for each game_id

SELECT
    payload:id::INT AS game_id,
    payload:season::INT AS season,
    payload:gameType::INT AS game_type,
    payload:gameDate::DATE AS game_date,
    payload:venue.default::STRING AS venue_name,
    payload:homeTeam.id::INT AS home_team_id,
    payload:homeTeam.abbrev::STRING AS home_team_abbrev,
    payload:homeTeam.name.default::STRING AS home_team_name,
    payload:awayTeam.id::INT AS away_team_id,
    payload:awayTeam.abbrev::STRING AS away_team_abbrev,
    payload:awayTeam.name.default::STRING AS away_team_name,
    payload:homeTeam.score::INT AS home_score,
    payload:awayTeam.score::INT AS away_score,
    CASE 
        WHEN payload:homeTeam.score > payload:awayTeam.score THEN home_team_id
        ELSE away_team_id
    END AS winning_team_id,
    payload:periodDescriptor.number::INT AS periods_played,
    s3_key AS source_s3_key,
    partition_date AS source_partition_date
FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC, s3_key DESC) = 1
