{{
  config(
    materialized='table',
    tags=['silver', 'odds', 'analytics']
  )
}}

-- Silver: Player SOG prop lines with actual outcomes
-- 
-- This model joins betting lines from The Odds API with actual player performance.
-- One row per player per game with:
--   - The betting line (e.g., 3.5)
--   - The over/under odds
--   - Actual SOG from the game
--   - Whether they hit the over
--   - Match confidence (how well we matched the player name)

WITH sog_lines AS (
    -- Get SOG lines, prioritizing major US bookmakers
    SELECT
        game_date,
        event_id,
        home_team,
        away_team,
        commence_time,
        player_name,
        line_value,
        bet_type,
        odds_american,
        bookmaker_key,
        line_last_update,
        -- Rank bookmakers by preference (DraftKings, FanDuel, others)
        ROW_NUMBER() OVER (
            PARTITION BY game_date, event_id, player_name, bet_type
            ORDER BY 
                CASE bookmaker_key
                    WHEN 'draftkings' THEN 1
                    WHEN 'fanduel' THEN 2
                    WHEN 'betmgm' THEN 3
                    WHEN 'caesars' THEN 4
                    ELSE 5
                END,
                line_last_update DESC
        ) AS book_rank
    FROM {{ ref('stg_odds_player_props') }}
    WHERE market = 'player_shots_on_goal'
),

-- Keep only top-ranked bookmaker per player/game/bet_type
best_lines AS (
    SELECT *
    FROM sog_lines
    WHERE book_rank = 1
),

-- Pivot over/under into columns
pivoted_lines AS (
    SELECT
        game_date,
        event_id,
        home_team,
        away_team,
        commence_time,
        player_name,
        bookmaker_key,
        MAX(line_value) AS sog_line,  -- Should be same for over/under
        MAX(CASE WHEN bet_type = 'Over' THEN odds_american END) AS over_odds,
        MAX(CASE WHEN bet_type = 'Under' THEN odds_american END) AS under_odds,
        MAX(line_last_update) AS line_last_update
    FROM best_lines
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- Join with player name crosswalk to get NHL player IDs
lines_with_player_id AS (
    SELECT
        pl.*,
        xw.nhl_player_id,
        xw.nhl_player_name,
        xw.match_method,
        xw.confidence AS name_match_confidence
    FROM pivoted_lines pl
    LEFT JOIN {{ ref('stg_player_name_crosswalk') }} xw
        ON pl.player_name = xw.odds_player_name
),

-- Get actual player stats for each game
player_actuals AS (
    SELECT
        g.game_date,
        g.game_id,
        pgs.player_id,
        p.player_name,
        pgs.shots AS shots_on_goal,
        t.team_name,
        t.team_abbrev,
        CASE 
            WHEN pgs.team_id = g.home_team_id THEN 'home'
            ELSE 'away'
        END AS home_away
    FROM {{ ref('fact_player_game_stats') }} pgs
    JOIN {{ ref('stg_games') }} g ON pgs.game_id = g.game_id
    JOIN {{ ref('dim_player') }} p ON pgs.player_id = p.player_id
    JOIN {{ ref('dim_team') }} t ON pgs.team_id = t.team_id
    WHERE pgs.shots IS NOT NULL
),

-- Join lines with actuals using player_id from crosswalk
joined AS (
    SELECT
        l.game_date,
        l.event_id,
        l.home_team AS odds_home_team,
        l.away_team AS odds_away_team,
        l.player_name AS odds_player_name,
        l.bookmaker_key,
        l.sog_line,
        l.over_odds,
        l.under_odds,
        l.line_last_update,
        l.match_method,
        l.name_match_confidence,
        a.player_name AS nhl_player_name,
        a.player_id AS nhl_player_id,
        a.team_name,
        a.team_abbrev,
        a.home_away,
        a.shots_on_goal AS actual_sog,
        a.game_id
    FROM lines_with_player_id l
    LEFT JOIN player_actuals a
        ON l.nhl_player_id = a.player_id
        AND l.game_date = a.game_date
)

SELECT
    game_date,
    event_id,
    odds_home_team,
    odds_away_team,
    odds_player_name,
    bookmaker_key,
    sog_line,
    over_odds,
    under_odds,
    line_last_update,
    
    -- Matching info
    match_method,
    name_match_confidence,
    nhl_player_name,
    nhl_player_id,
    team_name,
    team_abbrev,
    home_away,
    
    -- Actuals
    actual_sog,
    game_id,
    
    -- Outcome calculations
    CASE 
        WHEN actual_sog IS NULL AND nhl_player_id IS NULL THEN 'unmatched'  -- Couldn't match player
        WHEN actual_sog IS NULL THEN 'pending'  -- Game hasn't been played yet
        WHEN actual_sog > sog_line THEN 'over'
        WHEN actual_sog < sog_line THEN 'under'
        ELSE 'push'
    END AS outcome,
    
    CASE 
        WHEN actual_sog IS NULL THEN NULL
        WHEN actual_sog > sog_line THEN TRUE
        WHEN actual_sog < sog_line THEN FALSE
        ELSE NULL  -- push
    END AS hit_over,
    
    actual_sog - sog_line AS sog_vs_line,
    
    -- Betting outcome (implied probability and potential profit)
    -- American odds conversion: 
    --   Positive: profit = odds * stake / 100 (e.g., +150 = 1.5x stake profit)
    --   Negative: profit = 100 / abs(odds) * stake (e.g., -150 = 0.67x stake profit)
    CASE 
        WHEN over_odds > 0 THEN 100.0 / (over_odds + 100)  -- Positive odds
        WHEN over_odds < 0 THEN ABS(over_odds) / (ABS(over_odds) + 100.0)  -- Negative odds
    END AS over_implied_prob,
    
    CASE 
        WHEN under_odds > 0 THEN 100.0 / (under_odds + 100)
        WHEN under_odds < 0 THEN ABS(under_odds) / (ABS(under_odds) + 100.0)
    END AS under_implied_prob

FROM joined
