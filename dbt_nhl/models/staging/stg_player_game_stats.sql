{{
  config(
    materialized='view'
  )
}}

-- Extract player statistics from game boxscores
-- Combines forwards and defensemen from both home and away teams
-- Uses LATERAL FLATTEN to reduce code duplication

WITH latest_game_snapshots AS (
    SELECT
        payload:id::INT AS game_id,
        partition_date,
        payload
    FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC) = 1
),

team_player_combinations AS (
    SELECT
        game_id,
        partition_date,
        'home' AS home_away,
        'F' AS position_type,
        payload:homeTeam.forwards AS players
    FROM latest_game_snapshots
    
    UNION ALL
    
    SELECT
        game_id,
        partition_date,
        'home' AS home_away,
        'D' AS position_type,
        payload:homeTeam.defense AS players
    FROM latest_game_snapshots
    
    UNION ALL
    
    SELECT
        game_id,
        partition_date,
        'away' AS home_away,
        'F' AS position_type,
        payload:awayTeam.forwards AS players
    FROM latest_game_snapshots
    
    UNION ALL
    
    SELECT
        game_id,
        partition_date,
        'away' AS home_away,
        'D' AS position_type,
        payload:awayTeam.defense AS players
    FROM latest_game_snapshots
)

SELECT
    game_id,
    partition_date,
    p.value:playerId::INT AS player_id,
    p.value:name.default::STRING AS player_name,
    position_type,
    home_away,
    p.value:goals::INT AS goals,
    p.value:assists::INT AS assists,
    p.value:points::INT AS points,
    p.value:plusMinus::INT AS plus_minus,
    p.value:shots::INT AS shots,
    p.value:pim::INT AS penalty_minutes,
    p.value:toi::STRING AS time_on_ice,
    p.value:powerPlayGoals::INT AS pp_goals,
    p.value:shorthanded::INT AS sh_goals
FROM team_player_combinations,
LATERAL FLATTEN(input => players) p
WHERE p.value:playerId IS NOT NULL
