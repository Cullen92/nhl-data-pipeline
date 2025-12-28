{{
  config(
    materialized='table'
  )
}}

-- Player game-level statistics fact table
-- Joins with game dimension for additional context

SELECT
    pgs.game_id,
    pgs.player_id,
    pgs.player_name,
    pgs.position_type,
    pgs.home_away,
    g.game_date,
    g.season,
    CASE WHEN pgs.home_away = 'home' THEN g.home_team_id ELSE g.away_team_id END AS team_id,
    pgs.goals,
    pgs.assists,
    pgs.points,
    pgs.plus_minus,
    pgs.shots,
    pgs.penalty_minutes,
    pgs.time_on_ice,
    pgs.pp_goals,
    pgs.sh_goals,
    -- Calculate shooting percentage
    CASE 
        WHEN pgs.shots > 0 THEN ROUND((pgs.goals::FLOAT / pgs.shots) * 100, 2)
        ELSE 0
    END AS shooting_pct
FROM {{ ref('stg_player_game_stats') }} pgs
JOIN {{ ref('stg_games') }} g ON pgs.game_id = g.game_id
