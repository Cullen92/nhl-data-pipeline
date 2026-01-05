{{
  config(
    materialized='table',
    tags=['silver', 'bruins']
  )
}}

-- Find the Bruins' next upcoming game and opponent
-- This updates automatically when the schedule is refreshed

WITH latest_snapshot AS (
    -- Get only the most recent schedule snapshot
    SELECT payload
    FROM {{ source('raw_nhl', 'schedule_snapshots') }}
    ORDER BY ingest_ts DESC
    LIMIT 1
),

schedule_games AS (
    SELECT
        f.value:id::NUMBER AS game_id,
        d.value:date::DATE AS game_date,
        f.value:gameState::STRING AS game_state,
        f.value:homeTeam:abbrev::STRING AS home_team_abbrev,
        f.value:homeTeam:id::NUMBER AS home_team_id,
        f.value:awayTeam:abbrev::STRING AS away_team_abbrev,
        f.value:awayTeam:id::NUMBER AS away_team_id,
        f.value:gameType::NUMBER AS game_type
    FROM latest_snapshot,
        LATERAL FLATTEN(input => payload:gameWeek) d,
        LATERAL FLATTEN(input => d.value:games) f
    WHERE f.value:gameState::STRING IN ('FUT', 'PRE')  -- Future or pre-game
),

bruins_next_game AS (
    SELECT
        game_id,
        game_date,
        game_state,
        home_team_abbrev,
        home_team_id,
        away_team_abbrev,
        away_team_id,
        CASE 
            WHEN home_team_abbrev = 'BOS' THEN away_team_abbrev
            ELSE home_team_abbrev
        END AS opponent_abbrev,
        CASE 
            WHEN home_team_abbrev = 'BOS' THEN away_team_id
            ELSE home_team_id
        END AS opponent_team_id,
        CASE 
            WHEN home_team_abbrev = 'BOS' THEN 'home'
            ELSE 'away'
        END AS bruins_home_away
    FROM schedule_games
    WHERE home_team_abbrev = 'BOS' OR away_team_abbrev = 'BOS'
    ORDER BY game_date
    LIMIT 1
)

SELECT
    b.game_id,
    b.game_date,
    b.opponent_abbrev,
    b.opponent_team_id,
    b.bruins_home_away,
    t.team_name AS opponent_name,
    t.logo_url_light AS opponent_logo
FROM bruins_next_game b
LEFT JOIN {{ ref('dim_team') }} t ON b.opponent_team_id = t.team_id
