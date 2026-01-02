{{
  config(
    materialized='view'
  )
}}

-- Analytics view: Shots against by shooter position (Forward vs Defense)
-- Grain: One row per team per game
-- Shows how many shots a team allowed from forwards vs defensemen

WITH opponent_shots AS (
    -- Get shots taken AGAINST each team, grouped by shooter position
    SELECT
        fpgs.game_id,
        fpgs.date_key AS game_date,
        fpgs.opponent_team_id AS team_id,  -- The team being shot AT
        fpgs.team_id AS shooting_team_id,   -- The team taking the shot
        fpgs.position_type,                  -- F or D
        SUM(fpgs.shots) AS shots
    FROM {{ ref('fact_player_game_stats') }} fpgs
    GROUP BY 
        fpgs.game_id,
        fpgs.date_key,
        fpgs.opponent_team_id,
        fpgs.team_id,
        fpgs.position_type
),

-- Pivot to get forward and defense shots in columns
pivoted AS (
    SELECT
        game_id,
        game_date,
        team_id,
        shooting_team_id,
        SUM(CASE WHEN position_type = 'F' THEN shots ELSE 0 END) AS shots_against_forwards,
        SUM(CASE WHEN position_type = 'D' THEN shots ELSE 0 END) AS shots_against_defense,
        SUM(shots) AS total_shots_against
    FROM opponent_shots
    GROUP BY game_id, game_date, team_id, shooting_team_id
),

-- Add game context and rolling averages
with_context AS (
    SELECT
        p.game_id,
        p.game_date,
        p.team_id,
        t.team_abbrev,
        t.team_name,
        tgs.season,
        tgs.home_away,
        tgs.result,
        
        -- Shots against by position
        p.shots_against_forwards,
        p.shots_against_defense,
        p.total_shots_against,
        
        -- Percentages
        ROUND(100.0 * p.shots_against_forwards / NULLIF(p.total_shots_against, 0), 1) AS pct_shots_against_from_forwards,
        ROUND(100.0 * p.shots_against_defense / NULLIF(p.total_shots_against, 0), 1) AS pct_shots_against_from_defense,
        
        -- Rolling averages (3-game)
        ROUND(AVG(p.shots_against_forwards) OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS shots_against_forwards_3game_avg,
        
        ROUND(AVG(p.shots_against_defense) OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS shots_against_defense_3game_avg,
        
        -- Rolling averages (5-game)
        ROUND(AVG(p.shots_against_forwards) OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 2) AS shots_against_forwards_5game_avg,
        
        ROUND(AVG(p.shots_against_defense) OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 2) AS shots_against_defense_5game_avg,
        
        -- Season averages
        ROUND(AVG(p.shots_against_forwards) OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) AS shots_against_forwards_season_avg,
        
        ROUND(AVG(p.shots_against_defense) OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) AS shots_against_defense_season_avg,
        
        -- Game number in season
        ROW_NUMBER() OVER (
            PARTITION BY p.team_id, tgs.season 
            ORDER BY p.game_date, p.game_id
        ) AS games_played_in_season
        
    FROM pivoted p
    LEFT JOIN {{ ref('dim_team') }} t ON p.team_id = t.team_id
    LEFT JOIN {{ ref('fact_team_game_stats') }} tgs 
        ON p.game_id = tgs.game_id AND p.team_id = tgs.team_id
    WHERE tgs.game_type = 2  -- Regular season only
)

SELECT * FROM with_context
ORDER BY game_date DESC, team_id
