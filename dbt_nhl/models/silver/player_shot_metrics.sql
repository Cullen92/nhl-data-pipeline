{{
  config(
    materialized='view'
  )
}}

-- Analytics view: Player shot metrics aggregated by season with recent form
-- Designed as a daily snapshot for prediction models
-- Rolling averages only shown if player played in team's most recent N games
-- Grain: One row per player per season

WITH player_game_metrics AS (
    SELECT
        game_id,
        date_key AS game_date,
        -- Derive season: NHL season runs Oct-June, labeled as YYYYYYYY (e.g., 20242025)
        CASE 
            WHEN MONTH(date_key) >= 10 
            THEN (YEAR(date_key) * 10000) + (YEAR(date_key) + 1)
            ELSE ((YEAR(date_key) - 1) * 10000) + YEAR(date_key)
        END AS season,
        player_id,
        player_name,
        team_id,
        team_abbrev,
        position_code,
        position_type,
        shots,
        goals,
        assists,
        points,
        plus_minus,
        hits,
        blocked_shots,
        takeaways,
        giveaways,
        faceoff_win_pct,
        time_on_ice,
        shifts,
        pp_goals,
        sh_goals
    FROM {{ ref('fact_player_game_stats') }}
),

-- Rank each team's games within the season (most recent = 1)
-- Must get distinct games FIRST, then apply window functions
team_games AS (
    SELECT DISTINCT
        team_id,
        season,
        game_id,
        game_date
    FROM player_game_metrics
),

team_game_ranks AS (
    SELECT
        team_id,
        season,
        game_id,
        game_date,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season 
            ORDER BY game_date DESC, game_id DESC
        ) AS team_game_rank,
        COUNT(*) OVER (PARTITION BY team_id, season) AS team_total_games
    FROM team_games
),

-- Join player games with team game ranks
player_with_team_rank AS (
    SELECT
        pgm.*,
        tgr.team_game_rank,
        tgr.team_total_games
    FROM player_game_metrics pgm
    JOIN team_game_ranks tgr
        ON pgm.team_id = tgr.team_id
        AND pgm.season = tgr.season
        AND pgm.game_id = tgr.game_id
),

-- Calculate rolling averages and check if player was in team's recent games
rolling_metrics AS (
    SELECT
        player_id,
        season,
        team_id,
        MAX(team_total_games) AS team_total_games,
        
        -- Count how many of team's last N games player appeared in
        COUNT(CASE WHEN team_game_rank <= 3 THEN 1 END) AS games_in_team_last3,
        COUNT(CASE WHEN team_game_rank <= 5 THEN 1 END) AS games_in_team_last5,
        COUNT(CASE WHEN team_game_rank <= 10 THEN 1 END) AS games_in_team_last10,
        
        -- Rolling averages (only from player's games in team's last N)
        AVG(CASE WHEN team_game_rank <= 3 THEN shots END) AS shots_3game_avg,
        AVG(CASE WHEN team_game_rank <= 3 THEN goals END) AS goals_3game_avg,
        AVG(CASE WHEN team_game_rank <= 3 THEN points END) AS points_3game_avg,
        
        AVG(CASE WHEN team_game_rank <= 5 THEN shots END) AS shots_5game_avg,
        AVG(CASE WHEN team_game_rank <= 5 THEN goals END) AS goals_5game_avg,
        AVG(CASE WHEN team_game_rank <= 5 THEN points END) AS points_5game_avg,
        
        AVG(CASE WHEN team_game_rank <= 10 THEN shots END) AS shots_10game_avg,
        AVG(CASE WHEN team_game_rank <= 10 THEN goals END) AS goals_10game_avg,
        AVG(CASE WHEN team_game_rank <= 10 THEN points END) AS points_10game_avg
        
    FROM player_with_team_rank
    GROUP BY player_id, season, team_id
),

-- Season aggregates
season_totals AS (
    SELECT
        season,
        player_id,
        player_name,
        team_id,
        team_abbrev,
        position_code,
        position_type,
        
        -- Game count
        COUNT(*) AS games_played,
        
        -- Season totals
        SUM(shots) AS total_shots,
        SUM(goals) AS total_goals,
        SUM(assists) AS total_assists,
        SUM(points) AS total_points,
        SUM(plus_minus) AS total_plus_minus,
        SUM(hits) AS total_hits,
        SUM(blocked_shots) AS total_blocked_shots,
        SUM(takeaways) AS total_takeaways,
        SUM(giveaways) AS total_giveaways,
        SUM(shifts) AS total_shifts,
        SUM(pp_goals) AS total_pp_goals,
        SUM(sh_goals) AS total_sh_goals,
        
        -- Per-game averages (season)
        ROUND(AVG(shots), 2) AS shots_per_game,
        ROUND(AVG(goals), 2) AS goals_per_game,
        ROUND(AVG(assists), 2) AS assists_per_game,
        ROUND(AVG(points), 2) AS points_per_game,
        ROUND(AVG(plus_minus), 2) AS plus_minus_per_game,
        ROUND(AVG(hits), 2) AS hits_per_game,
        ROUND(AVG(blocked_shots), 2) AS blocked_shots_per_game,
        ROUND(AVG(takeaways), 2) AS takeaways_per_game,
        ROUND(AVG(giveaways), 2) AS giveaways_per_game,
        ROUND(AVG(faceoff_win_pct), 2) AS faceoff_win_pct_avg,
        ROUND(AVG(shifts), 2) AS shifts_per_game,
        
        -- Shooting percentage (goals / shots)
        CASE 
            WHEN SUM(shots) > 0 
            THEN ROUND(100.0 * SUM(goals) / SUM(shots), 2)
            ELSE 0 
        END AS shooting_pct

    FROM player_game_metrics
    GROUP BY
        season,
        player_id,
        player_name,
        team_id,
        team_abbrev,
        position_code,
        position_type
)

SELECT
    st.*,
    
    -- Recent form: only show if player played in ALL of team's last N games
    -- and team has played enough games for the window to be meaningful
    CASE WHEN rm.games_in_team_last3 = LEAST(3, rm.team_total_games) AND rm.team_total_games >= 3
        THEN ROUND(rm.shots_3game_avg, 2) END AS shots_last3_avg,
    CASE WHEN rm.games_in_team_last3 = LEAST(3, rm.team_total_games) AND rm.team_total_games >= 3
        THEN ROUND(rm.goals_3game_avg, 2) END AS goals_last3_avg,
    CASE WHEN rm.games_in_team_last3 = LEAST(3, rm.team_total_games) AND rm.team_total_games >= 3
        THEN ROUND(rm.points_3game_avg, 2) END AS points_last3_avg,
    CASE WHEN rm.games_in_team_last5 = LEAST(5, rm.team_total_games) AND rm.team_total_games >= 5
        THEN ROUND(rm.shots_5game_avg, 2) END AS shots_last5_avg,
    CASE WHEN rm.games_in_team_last5 = LEAST(5, rm.team_total_games) AND rm.team_total_games >= 5
        THEN ROUND(rm.goals_5game_avg, 2) END AS goals_last5_avg,
    CASE WHEN rm.games_in_team_last5 = LEAST(5, rm.team_total_games) AND rm.team_total_games >= 5
        THEN ROUND(rm.points_5game_avg, 2) END AS points_last5_avg,
    CASE WHEN rm.games_in_team_last10 = LEAST(10, rm.team_total_games) AND rm.team_total_games >= 10
        THEN ROUND(rm.shots_10game_avg, 2) END AS shots_last10_avg,
    CASE WHEN rm.games_in_team_last10 = LEAST(10, rm.team_total_games) AND rm.team_total_games >= 10
        THEN ROUND(rm.goals_10game_avg, 2) END AS goals_last10_avg,
    CASE WHEN rm.games_in_team_last10 = LEAST(10, rm.team_total_games) AND rm.team_total_games >= 10
        THEN ROUND(rm.points_10game_avg, 2) END AS points_last10_avg,
    
    -- Expose games played in recent stretch for transparency
    rm.games_in_team_last3,
    rm.games_in_team_last5,
    rm.games_in_team_last10,
    rm.team_total_games

FROM season_totals st
LEFT JOIN rolling_metrics rm
    ON st.player_id = rm.player_id
    AND st.season = rm.season
    AND st.team_id = rm.team_id
