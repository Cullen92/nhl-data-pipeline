{{
  config(
    materialized='table',
    tags=['silver', 'shots', 'tableau']
  )
}}

-- Tableau Heatmap: Team shot locations (offense - shots taken)
-- Aggregated shot data per team with coordinates for rink overlay visualization
-- Grain: One row per team per (x,y) coordinate bin per season

WITH shot_data AS (
    SELECT
        shooting_team_id,
        defending_team_id,
        season,
        
        -- Bin coordinates into 2-foot cells for manageable aggregation
        ROUND(x_coord_normalized / 2) * 2 AS x_bin,
        ROUND(y_coord_normalized / 2) * 2 AS y_bin,
        
        shot_result,
        is_goal,
        shot_type,
        shooter_position_type
        
    FROM {{ ref('fact_shot_events') }}
    WHERE x_coord IS NOT NULL
      AND y_coord IS NOT NULL
),

-- Team offensive shots (shots TAKEN by team)
offensive_shots AS (
    SELECT
        shooting_team_id AS team_id,
        season,
        x_bin,
        y_bin,
        'offense' AS shot_context,
        
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
        
        -- By position
        SUM(CASE WHEN shooter_position_type = 'F' THEN 1 ELSE 0 END) AS forward_shots,
        SUM(CASE WHEN shooter_position_type = 'D' THEN 1 ELSE 0 END) AS defense_shots,
        
        -- Shot type breakdown
        SUM(CASE WHEN shot_type = 'wrist' THEN 1 ELSE 0 END) AS wrist_shots,
        SUM(CASE WHEN shot_type = 'slap' THEN 1 ELSE 0 END) AS slap_shots,
        SUM(CASE WHEN shot_type = 'snap' THEN 1 ELSE 0 END) AS snap_shots,
        SUM(CASE WHEN shot_type = 'backhand' THEN 1 ELSE 0 END) AS backhand_shots
        
    FROM shot_data
    GROUP BY shooting_team_id, season, x_bin, y_bin
),

-- Team defensive shots (shots AGAINST team)
defensive_shots AS (
    SELECT
        defending_team_id AS team_id,
        season,
        x_bin,
        y_bin,
        'defense' AS shot_context,
        
        COUNT(*) AS total_shots,
        SUM(is_goal) AS goals,
        SUM(CASE WHEN shot_result = 'saved' THEN 1 ELSE 0 END) AS shots_on_goal,
        SUM(CASE WHEN shot_result = 'missed' THEN 1 ELSE 0 END) AS missed_shots,
        
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND(SUM(is_goal) * 100.0 / COUNT(*), 2)
            ELSE 0 
        END AS goals_against_pct,
        
        SUM(CASE WHEN shooter_position_type = 'F' THEN 1 ELSE 0 END) AS forward_shots,
        SUM(CASE WHEN shooter_position_type = 'D' THEN 1 ELSE 0 END) AS defense_shots,
        
        SUM(CASE WHEN shot_type = 'wrist' THEN 1 ELSE 0 END) AS wrist_shots,
        SUM(CASE WHEN shot_type = 'slap' THEN 1 ELSE 0 END) AS slap_shots,
        SUM(CASE WHEN shot_type = 'snap' THEN 1 ELSE 0 END) AS snap_shots,
        SUM(CASE WHEN shot_type = 'backhand' THEN 1 ELSE 0 END) AS backhand_shots
        
    FROM shot_data
    WHERE defending_team_id IS NOT NULL
    GROUP BY defending_team_id, season, x_bin, y_bin
),

-- Combine offense and defense
combined AS (
    SELECT * FROM offensive_shots
    UNION ALL
    SELECT 
        team_id,
        season,
        x_bin,
        y_bin,
        shot_context,
        total_shots,
        goals,
        shots_on_goal,
        missed_shots,
        goals_against_pct AS shooting_pct,  -- Rename for union compatibility
        forward_shots,
        defense_shots,
        wrist_shots,
        slap_shots,
        snap_shots,
        backhand_shots
    FROM defensive_shots
)

SELECT
    c.team_id,
    t.team_abbrev,
    t.team_name,
    c.season,
    c.shot_context,
    
    -- Coordinates for Tableau rink overlay
    c.x_bin,
    c.y_bin,
    
    -- Metrics
    c.total_shots,
    c.goals,
    c.shots_on_goal,
    c.missed_shots,
    c.shooting_pct,
    
    -- By shooter position
    c.forward_shots,
    c.defense_shots,
    
    -- Shot types
    c.wrist_shots,
    c.slap_shots,
    c.snap_shots,
    c.backhand_shots

FROM combined c
LEFT JOIN {{ ref('dim_team') }} t ON c.team_id = t.team_id
ORDER BY c.team_id, c.season, c.shot_context, c.total_shots DESC

