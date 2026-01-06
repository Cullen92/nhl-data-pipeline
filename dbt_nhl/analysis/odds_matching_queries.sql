-- Odds vs Actuals Matching Analysis
-- Run these queries in Snowflake to analyze betting outcomes

-- ============================================================================
-- 1. OVERVIEW: Match rate between odds and NHL player names
-- ============================================================================

-- First, check the crosswalk quality
SELECT
    match_method,
    COUNT(*) AS num_players,
    ROUND(AVG(confidence) * 100, 1) AS avg_confidence_pct
FROM {{ ref('stg_player_name_crosswalk') }}
GROUP BY 1
ORDER BY avg_confidence_pct DESC;

-- ============================================================================
-- 2. SAMPLE: See how names are being matched
-- ============================================================================

SELECT
    odds_player_name,
    nhl_player_name,
    match_method,
    confidence,
    current_team_abbrev
FROM {{ ref('stg_player_name_crosswalk') }}
ORDER BY match_method, odds_player_name
LIMIT 50;

-- ============================================================================
-- 3. UNMATCHED: Find odds players we couldn't match
-- ============================================================================

SELECT DISTINCT
    p.player_name AS odds_player_name,
    p.game_date
FROM {{ ref('stg_odds_player_props') }} p
LEFT JOIN {{ ref('stg_player_name_crosswalk') }} xw
    ON p.player_name = xw.odds_player_name
WHERE xw.odds_player_name IS NULL
  AND p.market = 'player_shots_on_goal'
ORDER BY p.player_name
LIMIT 100;

-- ============================================================================
-- 4. OVERALL BETTING PERFORMANCE
-- ============================================================================

WITH matched_props AS (
    SELECT *
    FROM {{ ref('fact_player_sog_props_v2') }}
    WHERE match_method IS NOT NULL
      AND actual_sog IS NOT NULL
)

SELECT
    COUNT(*) AS total_props,
    COUNT(CASE WHEN outcome = 'over' THEN 1 END) AS overs_hit,
    COUNT(CASE WHEN outcome = 'under' THEN 1 END) AS unders_hit,
    COUNT(CASE WHEN outcome = 'push' THEN 1 END) AS pushes,
    
    -- Over hit rate (excluding pushes)
    ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct,
    
    -- Average stats
    ROUND(AVG(sog_line), 2) AS avg_line,
    ROUND(AVG(actual_sog), 2) AS avg_actual_sog,
    ROUND(AVG(sog_vs_line), 2) AS avg_beat_line_by,
    
    -- Implied probability from odds
    ROUND(AVG(over_implied_prob) * 100, 1) AS avg_over_implied_prob_pct
FROM matched_props;

-- ============================================================================
-- 5. PERFORMANCE BY LINE VALUE
-- ============================================================================

WITH matched_props AS (
    SELECT *
    FROM {{ ref('fact_player_sog_props_v2') }}
    WHERE match_method IS NOT NULL
      AND actual_sog IS NOT NULL
)

SELECT
    sog_line,
    COUNT(*) AS n_props,
    COUNT(CASE WHEN outcome = 'over' THEN 1 END) AS overs_hit,
    ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct,
    ROUND(AVG(actual_sog), 2) AS avg_actual_sog,
    ROUND(AVG(over_implied_prob) * 100, 1) AS avg_over_implied_prob_pct,
    -- Edge = actual hit rate - implied probability
    ROUND(
        100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0)
        - AVG(over_implied_prob) * 100, 
    1) AS edge_vs_books_pct
FROM matched_props
GROUP BY sog_line
HAVING COUNT(*) >= 10  -- Minimum sample
ORDER BY sog_line;

-- ============================================================================
-- 6. TOP PERFORMERS: Players who beat their lines
-- ============================================================================

WITH matched_props AS (
    SELECT *
    FROM {{ ref('fact_player_sog_props_v2') }}
    WHERE match_method IS NOT NULL
      AND actual_sog IS NOT NULL
)

SELECT
    nhl_player_name,
    team_abbrev,
    COUNT(*) AS n_props,
    ROUND(AVG(sog_line), 1) AS avg_line,
    ROUND(AVG(actual_sog), 1) AS avg_actual_sog,
    ROUND(AVG(sog_vs_line), 2) AS avg_beat_line_by,
    COUNT(CASE WHEN outcome = 'over' THEN 1 END) AS overs_hit,
    ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct
FROM matched_props
GROUP BY nhl_player_name, team_abbrev
HAVING COUNT(*) >= 5  -- Minimum sample
ORDER BY avg_beat_line_by DESC
LIMIT 25;

-- ============================================================================
-- 7. DAILY TREND: How has betting performed over time?
-- ============================================================================

WITH matched_props AS (
    SELECT *
    FROM {{ ref('fact_player_sog_props_v2') }}
    WHERE match_method IS NOT NULL
      AND actual_sog IS NOT NULL
)

SELECT
    game_date,
    COUNT(*) AS n_props,
    COUNT(CASE WHEN outcome = 'over' THEN 1 END) AS overs_hit,
    ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct,
    ROUND(AVG(actual_sog), 2) AS avg_actual_sog,
    ROUND(AVG(sog_line), 2) AS avg_line
FROM matched_props
GROUP BY game_date
ORDER BY game_date DESC
LIMIT 30;

-- ============================================================================
-- 8. SAMPLE PROPS WITH OUTCOMES
-- ============================================================================

SELECT
    game_date,
    odds_player_name,
    nhl_player_name,
    team_abbrev,
    sog_line,
    over_odds,
    actual_sog,
    outcome,
    sog_vs_line,
    match_method
FROM {{ ref('fact_player_sog_props_v2') }}
WHERE match_method IS NOT NULL
  AND actual_sog IS NOT NULL
ORDER BY game_date DESC, outcome DESC
LIMIT 50;
