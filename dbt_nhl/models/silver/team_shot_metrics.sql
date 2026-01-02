{{
  config(
    materialized='view'
  )
}}

-- Analytics view: Team shot metrics with rolling averages
-- Provides easy-to-query team shot statistics and averages over different time windows

WITH team_game_metrics AS (
    SELECT
        game_id,
        date_key AS game_date,
        season,
        team_id,
        home_away,
        result,
        shots_for,
        shots_against,
        shot_differential,
        shooting_pct,
        save_pct,
        goals_for,
        goals_against
    FROM {{ ref('fact_team_game_stats') }}
    WHERE game_type = 2  -- Regular season only for cleaner averages
),

rolling_averages AS (
    SELECT
        *,
        
        -- 3-game rolling averages
        AVG(shots_against) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS shots_against_3game_avg,
        
        -- 5-game rolling averages
        AVG(shots_for) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS shots_for_5game_avg,
        
        AVG(shots_against) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS shots_against_5game_avg,
        
        -- 10-game rolling averages
        AVG(shots_for) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS shots_for_10game_avg,
        
        AVG(shots_against) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS shots_against_10game_avg,
        
        -- Season-to-date averages
        AVG(shots_for) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS shots_for_season_avg,
        
        AVG(shots_against) OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS shots_against_season_avg,
        
        -- Game count for the team in season (useful for filtering min games)
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date, game_id
        ) AS games_played_in_season
        
    FROM team_game_metrics
)

SELECT
    game_id,
    game_date,
    season,
    team_id,
    home_away,
    result,
    games_played_in_season,
    
    -- Current game stats
    shots_for,
    shots_against,
    shot_differential,
    shooting_pct,
    save_pct,
    goals_for,
    goals_against,
    
    -- 3-game rolling averages
    ROUND(shots_against_3game_avg, 2) AS shots_against_3game_avg,
    
    -- 5-game rolling averages
    ROUND(shots_for_5game_avg, 2) AS shots_for_5game_avg,
    ROUND(shots_against_5game_avg, 2) AS shots_against_5game_avg,
    
    -- 10-game rolling averages
    ROUND(shots_for_10game_avg, 2) AS shots_for_10game_avg,
    ROUND(shots_against_10game_avg, 2) AS shots_against_10game_avg,
    
    -- Season averages
    ROUND(shots_for_season_avg, 2) AS shots_for_season_avg,
    ROUND(shots_against_season_avg, 2) AS shots_against_season_avg

FROM rolling_averages
