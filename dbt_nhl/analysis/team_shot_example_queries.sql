-- Team Shot Statistics - Example Queries
-- Use these queries to analyze team-level shot performance

-- =============================================================================
-- 1. BASIC TEAM SHOT METRICS
-- =============================================================================

-- Get latest game stats for all teams
SELECT 
    game_date,
    team_id,
    home_away,
    result,
    shots_for,
    shots_against,
    shot_differential,
    shooting_pct,
    goals_for
FROM nhl_analytics.silver.fact_team_game_stats
WHERE season = 20242025
ORDER BY game_date DESC, team_id
LIMIT 50;

-- =============================================================================
-- 2. TEAM SHOT VOLUME LEADERS
-- =============================================================================

-- Teams with highest average shots per game (current season, min 10 games)
SELECT 
    team_id,
    COUNT(*) AS games_played,
    ROUND(AVG(shots_for), 2) AS avg_shots_per_game,
    ROUND(AVG(shots_against), 2) AS avg_shots_against,
    ROUND(AVG(shot_differential), 2) AS avg_shot_diff
FROM nhl_analytics.silver.fact_team_game_stats
WHERE season = 20242025
    AND game_type = 2  -- Regular season
GROUP BY team_id
HAVING games_played >= 10
ORDER BY avg_shots_per_game DESC;

-- =============================================================================
-- 3. SHOT EFFICIENCY ANALYSIS
-- =============================================================================

-- Teams with best shooting percentage (min 20 games)
SELECT 
    team_id,
    COUNT(*) AS games,
    ROUND(AVG(shooting_pct), 2) AS avg_shooting_pct,
    ROUND(AVG(save_pct), 2) AS avg_save_pct,
    SUM(goals_for) AS total_goals,
    SUM(shots_for) AS total_shots
FROM nhl_analytics.silver.fact_team_game_stats
WHERE season = 20242025
    AND game_type = 2
GROUP BY team_id
HAVING games >= 20
ORDER BY avg_shooting_pct DESC;

-- =============================================================================
-- 4. ROLLING AVERAGE TRENDS
-- =============================================================================

-- Recent performance trends (last 10 games) using rolling averages
SELECT 
    game_date,
    team_id,
    shots_for,
    shots_for_5game_avg,
    shots_for_10game_avg,
    shots_for_season_avg,
    -- Trend indicators
    shots_for - shots_for_season_avg AS vs_season_avg,
    shots_for_5game_avg - shots_for_10game_avg AS short_term_trend
FROM nhl_analytics.silver.team_shot_metrics
WHERE team_id = 10  -- Replace with desired team
    AND season = 20242025
ORDER BY game_date DESC
LIMIT 10;

-- =============================================================================
-- 5. HOT/COLD STREAK DETECTION
-- =============================================================================

-- Teams currently on hot shooting streaks (5-game avg > season avg)
SELECT 
    team_id,
    game_date,
    games_played_in_season,
    shots_for_5game_avg,
    shots_for_season_avg,
    ROUND(shots_for_5game_avg - shots_for_season_avg, 2) AS above_average
FROM nhl_analytics.silver.team_shot_metrics
WHERE games_played_in_season >= 20  -- Ensure meaningful sample
    AND shots_for_5game_avg > shots_for_season_avg + 2  -- At least 2 shots above average
    -- Get most recent game for each team
    AND (team_id, game_date) IN (
        SELECT team_id, MAX(game_date)
        FROM nhl_analytics.silver.team_shot_metrics
        WHERE games_played_in_season >= 20
        GROUP BY team_id
    )
ORDER BY above_average DESC;

-- =============================================================================
-- 6. SHOT DIFFERENTIAL ANALYSIS
-- =============================================================================

-- Best and worst shot differential teams
SELECT 
    team_id,
    COUNT(*) AS games,
    ROUND(AVG(shots_for), 2) AS avg_shots_for,
    ROUND(AVG(shots_against), 2) AS avg_shots_against,
    ROUND(AVG(shot_differential), 2) AS avg_shot_diff,
    -- Correlated with winning?
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
    ROUND(SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100, 1) AS win_pct
FROM nhl_analytics.silver.fact_team_game_stats
WHERE season = 20242025
    AND game_type = 2
GROUP BY team_id
HAVING games >= 10
ORDER BY avg_shot_diff DESC;

-- =============================================================================
-- 7. HOME VS AWAY SHOT PERFORMANCE
-- =============================================================================

-- Compare team performance at home vs away
WITH home_stats AS (
    SELECT 
        team_id,
        AVG(shots_for) AS home_shots_avg,
        AVG(shots_against) AS home_against_avg,
        COUNT(*) AS home_games
    FROM nhl_analytics.silver.fact_team_game_stats
    WHERE home_away = 'home' AND season = 20242025 AND game_type = 2
    GROUP BY team_id
),
away_stats AS (
    SELECT 
        team_id,
        AVG(shots_for) AS away_shots_avg,
        AVG(shots_against) AS away_against_avg,
        COUNT(*) AS away_games
    FROM nhl_analytics.silver.fact_team_game_stats
    WHERE home_away = 'away' AND season = 20242025 AND game_type = 2
    GROUP BY team_id
)
SELECT 
    h.team_id,
    ROUND(h.home_shots_avg, 2) AS home_shots,
    ROUND(a.away_shots_avg, 2) AS away_shots,
    ROUND(h.home_shots_avg - a.away_shots_avg, 2) AS home_advantage,
    ROUND(h.home_against_avg, 2) AS home_against,
    ROUND(a.away_against_avg, 2) AS away_against
FROM home_stats h
JOIN away_stats a ON h.team_id = a.team_id
WHERE h.home_games >= 5 AND a.away_games >= 5
ORDER BY home_advantage DESC;

-- =============================================================================
-- 8. SHOT ATTEMPTS AND BLOCKED SHOTS
-- =============================================================================

-- Teams generating most shot attempts (including blocked)
SELECT 
    team_id,
    COUNT(*) AS games,
    ROUND(AVG(shot_attempts_for), 2) AS avg_shot_attempts,
    ROUND(AVG(shots_for), 2) AS avg_shots_on_goal,
    ROUND(AVG(blocked_shots), 2) AS avg_blocked,
    -- What % of attempts actually become shots on goal
    ROUND(AVG(shots_for) / NULLIF(AVG(shot_attempts_for), 0) * 100, 1) AS shot_success_rate
FROM nhl_analytics.silver.fact_team_game_stats
WHERE season = 20242025 AND game_type = 2
GROUP BY team_id
HAVING games >= 10
ORDER BY avg_shot_attempts DESC;

-- =============================================================================
-- 9. RECENT FORM COMPARISON
-- =============================================================================

-- Compare multiple teams' recent form (last 5 games)
SELECT 
    team_id,
    ROUND(AVG(shots_for), 2) AS recent_shots_for,
    ROUND(AVG(shots_against), 2) AS recent_shots_against,
    ROUND(AVG(shooting_pct), 2) AS recent_shooting_pct,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins_last_5
FROM nhl_analytics.silver.fact_team_game_stats
WHERE season = 20242025
    AND game_type = 2
    AND game_date >= CURRENT_DATE - INTERVAL '15 days'  -- Adjust as needed
GROUP BY team_id
HAVING COUNT(*) >= 5
ORDER BY recent_shots_for DESC;

-- =============================================================================
-- 10. PREDICTIVE FEATURES
-- =============================================================================

-- Shot metrics as predictive features for wins
-- (Good candidates for ML models)
SELECT 
    team_id,
    game_date,
    -- Current game
    shots_for,
    shots_against,
    shot_differential,
    shooting_pct,
    -- Recent trends
    shots_for_5game_avg,
    shots_against_5game_avg,
    -- Season context
    shots_for_season_avg,
    games_played_in_season,
    -- Outcome
    result,
    goals_for
FROM nhl_analytics.silver.team_shot_metrics
WHERE season = 20242025
    AND games_played_in_season >= 10  -- Enough history for meaningful averages
ORDER BY game_date DESC;
