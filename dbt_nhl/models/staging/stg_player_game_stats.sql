{{
  config(
    materialized='view'
  )
}}

-- Extract player statistics from game boxscores
-- Combines forwards and defensemen from both home and away teams

WITH home_forwards AS (
    SELECT
        payload:id::INT AS game_id,
        partition_date,
        p.value:playerId::INT AS player_id,
        p.value:name.default::STRING AS player_name,
        'F' AS position_type,
        'home' AS home_away,
        p.value:goals::INT AS goals,
        p.value:assists::INT AS assists,
        p.value:points::INT AS points,
        p.value:plusMinus::INT AS plus_minus,
        p.value:shots::INT AS shots,
        p.value:pim::INT AS penalty_minutes,
        p.value:toi::STRING AS time_on_ice,
        p.value:powerPlayGoals::INT AS pp_goals,
        p.value:shorthanded::INT AS sh_goals
    FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }},
    LATERAL FLATTEN(input => payload:homeTeam.forwards) p
    WHERE p.value:playerId IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC) = 1
),

home_defense AS (
    SELECT
        payload:id::INT AS game_id,
        partition_date,
        p.value:playerId::INT AS player_id,
        p.value:name.default::STRING AS player_name,
        'D' AS position_type,
        'home' AS home_away,
        p.value:goals::INT AS goals,
        p.value:assists::INT AS assists,
        p.value:points::INT AS points,
        p.value:plusMinus::INT AS plus_minus,
        p.value:shots::INT AS shots,
        p.value:pim::INT AS penalty_minutes,
        p.value:toi::STRING AS time_on_ice,
        p.value:powerPlayGoals::INT AS pp_goals,
        p.value:shorthanded::INT AS sh_goals
    FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }},
    LATERAL FLATTEN(input => payload:homeTeam.defense) p
    WHERE p.value:playerId IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC) = 1
),

away_forwards AS (
    SELECT
        payload:id::INT AS game_id,
        partition_date,
        p.value:playerId::INT AS player_id,
        p.value:name.default::STRING AS player_name,
        'F' AS position_type,
        'away' AS home_away,
        p.value:goals::INT AS goals,
        p.value:assists::INT AS assists,
        p.value:points::INT AS points,
        p.value:plusMinus::INT AS plus_minus,
        p.value:shots::INT AS shots,
        p.value:pim::INT AS penalty_minutes,
        p.value:toi::STRING AS time_on_ice,
        p.value:powerPlayGoals::INT AS pp_goals,
        p.value:shorthanded::INT AS sh_goals
    FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }},
    LATERAL FLATTEN(input => payload:awayTeam.forwards) p
    WHERE p.value:playerId IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC) = 1
),

away_defense AS (
    SELECT
        payload:id::INT AS game_id,
        partition_date,
        p.value:playerId::INT AS player_id,
        p.value:name.default::STRING AS player_name,
        'D' AS position_type,
        'away' AS home_away,
        p.value:goals::INT AS goals,
        p.value:assists::INT AS assists,
        p.value:points::INT AS points,
        p.value:plusMinus::INT AS plus_minus,
        p.value:shots::INT AS shots,
        p.value:pim::INT AS penalty_minutes,
        p.value:toi::STRING AS time_on_ice,
        p.value:powerPlayGoals::INT AS pp_goals,
        p.value:shorthanded::INT AS sh_goals
    FROM {{ source('raw_nhl', 'game_boxscore_snapshots') }},
    LATERAL FLATTEN(input => payload:awayTeam.defense) p
    WHERE p.value:playerId IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC) = 1
)

SELECT * FROM home_forwards
UNION ALL
SELECT * FROM home_defense
UNION ALL
SELECT * FROM away_forwards
UNION ALL
SELECT * FROM away_defense
