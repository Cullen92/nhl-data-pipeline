{{
  config(
    materialized='table',
    tags=['silver', 'shots', 'tableau']
  )
}}

-- Tableau Heatmap: Player shot locations
-- Aggregated shot data per player with coordinates for rink overlay visualization
-- Grain: One row per player per (x,y) coordinate bin per season

WITH shot_data AS (
    SELECT
        shooter_player_id,
        shooter_name,
        shooter_position_code,
        shooter_position_type,
        shooting_team_id,
        season,
        
        -- Bin coordinates into 2-foot cells for manageable aggregation
        -- NHL rink: 200ft x 85ft, center ice at (0,0)
        ROUND(x_coord_normalized / 2) * 2 AS x_bin,
        ROUND(y_coord_normalized / 2) * 2 AS y_bin,
        
        shot_result,
        is_goal,
        shot_type
        
    FROM {{ ref('fact_shot_events') }}
    WHERE shooter_player_id IS NOT NULL
      AND x_coord IS NOT NULL
      AND y_coord IS NOT NULL
),

aggregated AS (
    SELECT
        shooter_player_id,
        shooter_name,
        shooter_position_code,
        shooter_position_type,
        shooting_team_id,
        season,
        x_bin,
        y_bin,
        
        -- Shot counts by result
        COUNT(*) AS total_shots,
        SUM(is_goal) AS goals,
        SUM(CASE WHEN shot_result = 'saved' THEN 1 ELSE 0 END) AS shots_on_goal,
        SUM(CASE WHEN shot_result = 'missed' THEN 1 ELSE 0 END) AS missed_shots,
        
        -- Shooting percentage from this location
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND(SUM(is_goal) * 100.0 / COUNT(*), 2)
            ELSE 0 
        END AS shooting_pct,
        
        -- Shot type breakdown
        SUM(CASE WHEN shot_type = 'wrist' THEN 1 ELSE 0 END) AS wrist_shots,
        SUM(CASE WHEN shot_type = 'slap' THEN 1 ELSE 0 END) AS slap_shots,
        SUM(CASE WHEN shot_type = 'snap' THEN 1 ELSE 0 END) AS snap_shots,
        SUM(CASE WHEN shot_type = 'backhand' THEN 1 ELSE 0 END) AS backhand_shots,
        SUM(CASE WHEN shot_type = 'tip-in' THEN 1 ELSE 0 END) AS tip_in_shots,
        SUM(CASE WHEN shot_type = 'deflected' THEN 1 ELSE 0 END) AS deflected_shots,
        SUM(CASE WHEN shot_type = 'wrap-around' THEN 1 ELSE 0 END) AS wrap_around_shots
        
    FROM shot_data
    GROUP BY
        shooter_player_id,
        shooter_name,
        shooter_position_code,
        shooter_position_type,
        shooting_team_id,
        season,
        x_bin,
        y_bin
)

SELECT
    shooter_player_id,
    shooter_name,
    shooter_position_code,
    shooter_position_type,
    shooting_team_id,
    t.team_abbrev AS team_abbrev,
    t.team_name AS team_name,
    season,
    
    -- Coordinates for Tableau (use x_bin, y_bin for rink overlay)
    x_bin,
    y_bin,
    
    -- Metrics
    total_shots,
    goals,
    shots_on_goal,
    missed_shots,
    shooting_pct,
    
    -- Shot types
    wrist_shots,
    slap_shots,
    snap_shots,
    backhand_shots,
    tip_in_shots,
    deflected_shots,
    wrap_around_shots

FROM aggregated a
LEFT JOIN {{ ref('dim_team') }} t ON a.shooting_team_id = t.team_id
ORDER BY shooter_player_id, season, total_shots DESC

