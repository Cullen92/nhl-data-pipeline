{{
  config(
    materialized='table',
    tags=['silver', 'odds', 'analytics']
  )
}}

-- Silver: SOG Props Betting Performance Analysis
-- 
-- Aggregates betting outcomes to answer key questions:
-- 1. How often do overs hit? (by line, by player tier, etc.)
-- 2. What's the theoretical ROI if betting every over/under?
-- 3. Are certain line values more profitable than others?
-- 4. Which players consistently beat their lines?

WITH matched_props AS (
    -- Only include props where we successfully matched to a player
    SELECT *
    FROM {{ ref('fact_player_sog_props_v2') }}
    WHERE match_method IS NOT NULL
      AND actual_sog IS NOT NULL
),

-- Overall performance summary
overall_stats AS (
    SELECT
        COUNT(*) AS total_props,
        COUNT(CASE WHEN outcome = 'over' THEN 1 END) AS overs_hit,
        COUNT(CASE WHEN outcome = 'under' THEN 1 END) AS unders_hit,
        COUNT(CASE WHEN outcome = 'push' THEN 1 END) AS pushes,
        
        -- Hit rates
        ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
              NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct,
        
        -- Average stats
        ROUND(AVG(sog_line), 2) AS avg_line,
        ROUND(AVG(actual_sog), 2) AS avg_actual_sog,
        ROUND(AVG(sog_vs_line), 2) AS avg_sog_vs_line,
        
        -- Implied probability analysis
        ROUND(AVG(over_implied_prob) * 100, 1) AS avg_over_implied_prob_pct,
        ROUND(AVG(under_implied_prob) * 100, 1) AS avg_under_implied_prob_pct
    FROM matched_props
),

-- Performance by line value bucket
line_performance AS (
    SELECT
        CASE 
            WHEN sog_line <= 1.5 THEN '0.5-1.5'
            WHEN sog_line <= 2.5 THEN '2.5'
            WHEN sog_line <= 3.5 THEN '3.5'
            WHEN sog_line <= 4.5 THEN '4.5'
            WHEN sog_line <= 5.5 THEN '5.5'
            ELSE '6.0+'
        END AS line_bucket,
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
        1) AS over_edge_pct
    FROM matched_props
    GROUP BY 1, 2
    HAVING COUNT(*) >= 5  -- Only show lines with enough sample
    ORDER BY sog_line
),

-- Performance by match confidence
match_quality AS (
    SELECT
        match_method,
        COUNT(*) AS n_props,
        ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
              NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct,
        ROUND(AVG(name_match_confidence) * 100, 1) AS avg_confidence_pct
    FROM matched_props
    GROUP BY 1
    ORDER BY AVG(name_match_confidence) DESC
),

-- Top performers (players who consistently beat their lines)
player_performance AS (
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
    GROUP BY 1, 2
    HAVING COUNT(*) >= 5  -- Minimum sample size
),

-- Daily performance (for trend analysis)
daily_performance AS (
    SELECT
        game_date,
        COUNT(*) AS n_props,
        COUNT(CASE WHEN outcome = 'over' THEN 1 END) AS overs_hit,
        ROUND(100.0 * COUNT(CASE WHEN outcome = 'over' THEN 1 END) / 
              NULLIF(COUNT(CASE WHEN outcome != 'push' THEN 1 END), 0), 1) AS over_hit_rate_pct
    FROM matched_props
    GROUP BY 1
    ORDER BY game_date
)

-- Return overall stats with details
SELECT
    'overall' AS analysis_type,
    NULL AS segment,
    total_props,
    overs_hit,
    unders_hit,
    pushes,
    over_hit_rate_pct,
    avg_line,
    avg_actual_sog,
    avg_sog_vs_line,
    avg_over_implied_prob_pct,
    NULL AS edge_pct,
    NULL AS rank_in_segment
FROM overall_stats
