{{
  config(
    materialized='table'
  )
}}

-- Silver layer: Team game statistics fact table
-- Grain: One row per team per game (two rows per game - home & away)
-- Contains aggregated team performance metrics including shots and shot attempts

WITH latest_snapshots AS (
    -- Get most recent snapshot for each game
    SELECT
        payload:id::INT AS game_id,
        payload:gameDate::DATE AS game_date,
        payload:season::INT AS season,
        payload:gameType::INT AS game_type,
        payload:homeTeam.id::INT AS home_team_id,
        payload:homeTeam.abbrev::STRING AS home_team_abbrev,
        payload:homeTeam.score::INT AS home_score,
        payload:homeTeam.sog::INT AS home_sog,
        payload:awayTeam.id::INT AS away_team_id,
        payload:awayTeam.abbrev::STRING AS away_team_abbrev,
        payload:awayTeam.score::INT AS away_score,
        payload:awayTeam.sog::INT AS away_sog,
        payload:gameState::STRING AS game_state,
        payload
    FROM {{ ref('bronze_game_boxscore_snapshots') }}
    WHERE payload:gameState = 'OFF'  -- Only completed games
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC, s3_key DESC) = 1
),

-- Aggregate player stats to get team-level shot attempts and other stats
team_aggregates AS (
    SELECT
        game_id,
        team_id,
        home_away,
        
        -- Aggregate player stats for team metrics
        SUM(hits) AS total_hits,
        SUM(giveaways) AS total_giveaways,
        SUM(takeaways) AS total_takeaways,
        SUM(penalty_minutes) AS total_pim,
        SUM(goals) AS total_goals,
        SUM(pp_goals) AS total_pp_goals,
        SUM(sh_goals) AS total_sh_goals
        
    FROM {{ ref('fact_player_game_stats') }}
    GROUP BY game_id, team_id, home_away
),

-- Union home and away team stats
-- team_id = home team | opponent_team_id = away team
-- goals/shots for = for home team | goals/shots against = for away team
team_stats AS (
    -- Home team stats
    SELECT
        ls.game_id,
        ls.game_date,
        ls.season,
        ls.game_type,
        ls.home_team_id AS team_id,
        ls.away_team_id AS opponent_team_id,
        'home' AS home_away,
        ls.home_score AS goals_for,
        ls.away_score AS goals_against,
        ls.home_sog AS shots_for,
        ls.away_sog AS shots_against,
        
        -- Calculate result
        CASE 
            WHEN ls.home_score > ls.away_score THEN 'W'
            WHEN ls.home_score < ls.away_score THEN 'L'
            ELSE NULL -- Should never happen in completed games (no ties in NHL)
        END AS result,
        
        -- Goal differential
        ls.home_score - ls.away_score AS goal_differential
        
    FROM latest_snapshots ls
    
    UNION
    
    -- Away team stats
    SELECT
        ls.game_id,
        ls.game_date,
        ls.season,
        ls.game_type,
        ls.away_team_id AS team_id, 
        ls.home_team_id AS opponent_team_id,
        'away' AS home_away,
        ls.away_score AS goals_for,
        ls.home_score AS goals_against,
        ls.away_sog AS shots_for,
        ls.home_sog AS shots_against,
        
        -- Calculate result
        CASE 
            WHEN ls.away_score > ls.home_score THEN 'W'
            WHEN ls.away_score < ls.home_score THEN 'L'
            ELSE NULL -- Should never happen in completed games (no ties in NHL)
        END AS result,
        
        -- Goal differential
        ls.away_score - ls.home_score AS goal_differential
        
    FROM latest_snapshots ls
)

SELECT
    -- Composite Primary Key
    ts.game_id,
    ts.team_id,
    
    -- Foreign Keys
    ts.game_date AS date_key,
    ts.opponent_team_id,
    
    -- Context
    ts.season,
    ts.game_type,
    ts.home_away,
    ts.result,
    
    -- Goals
    ts.goals_for,
    ts.goals_against,
    ts.goal_differential,
    
    -- Shots
    ts.shots_for,
    ts.shots_against,
    ts.shots_for - ts.shots_against AS shot_differential,
    
    -- Shooting percentage
    CASE 
        WHEN ts.shots_for > 0 THEN (ts.goals_for::FLOAT / ts.shots_for::FLOAT) * 100
        ELSE 0.0
    END AS shooting_pct,
    
    -- Save percentage (opponent's perspective)
    CASE 
        WHEN ts.shots_against > 0 THEN ((ts.shots_against - ts.goals_against)::FLOAT / ts.shots_against::FLOAT) * 100
        ELSE 0.0
    END AS save_pct,
    
    -- Other team stats from player aggregates
    COALESCE(ta.total_hits, 0) AS hits,
    COALESCE(ta.total_giveaways, 0) AS giveaways,
    COALESCE(ta.total_takeaways, 0) AS takeaways,
    COALESCE(ta.total_pim, 0) AS penalty_minutes,
    COALESCE(ta.total_pp_goals, 0) AS pp_goals,
    COALESCE(ta.total_sh_goals, 0) AS sh_goals

FROM team_stats ts
LEFT JOIN team_aggregates ta ON ts.game_id = ta.game_id AND ts.team_id = ta.team_id AND ts.home_away = ta.home_away
WHERE ts.game_id IS NOT NULL
  AND ts.team_id IS NOT NULL
ORDER BY ts.game_date DESC, ts.game_id, ts.team_id
