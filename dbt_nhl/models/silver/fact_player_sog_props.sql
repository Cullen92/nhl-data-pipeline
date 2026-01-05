{{
  config(
    materialized='table',
    tags=['silver', 'odds', 'analytics']
  )
}}

-- Silver: Player SOG prop lines with actual outcomes
-- One row per player per game with:
--   - The betting line (e.g., 3.5)
--   - The over/under odds
--   - Actual SOG from the game
--   - Whether they hit the over

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

-- Join with actual player stats to get outcome
-- Note: We need to match on game_date and player name
-- This is imperfect since odds API uses display names, NHL API uses full names
player_actuals AS (
    SELECT
        g.game_date,
        p.player_name,
        pgs.shots_on_goal,
        pgs.game_id,
        t.team_name,
        CASE 
            WHEN pgs.team_id = g.home_team_id THEN 'home'
            ELSE 'away'
        END AS home_away
    FROM {{ ref('fact_player_game_stats') }} pgs
    JOIN {{ ref('stg_games') }} g ON pgs.game_id = g.game_id
    JOIN {{ ref('dim_player') }} p ON pgs.player_id = p.player_id
    JOIN {{ ref('dim_team') }} t ON pgs.team_id = t.team_id
    WHERE pgs.shots_on_goal IS NOT NULL
)

SELECT
    pl.game_date,
    pl.event_id,
    pl.home_team AS odds_home_team,
    pl.away_team AS odds_away_team,
    pl.player_name AS odds_player_name,
    pl.bookmaker_key,
    pl.sog_line,
    pl.over_odds,
    pl.under_odds,
    pl.line_last_update,
    pa.player_name AS nhl_player_name,
    pa.team_name,
    pa.home_away,
    pa.shots_on_goal AS actual_sog,
    pa.game_id,
    -- Outcome calculations
    CASE 
        WHEN pa.shots_on_goal > pl.sog_line THEN 'over'
        WHEN pa.shots_on_goal < pl.sog_line THEN 'under'
        ELSE 'push'
    END AS outcome,
    CASE 
        WHEN pa.shots_on_goal > pl.sog_line THEN TRUE
        WHEN pa.shots_on_goal < pl.sog_line THEN FALSE
        ELSE NULL  -- push
    END AS hit_over,
    pa.shots_on_goal - pl.sog_line AS sog_vs_line
FROM pivoted_lines pl
LEFT JOIN player_actuals pa
    ON pl.game_date = pa.game_date
    -- Fuzzy match on player name (odds API often has different formatting)
    AND (
        UPPER(pl.player_name) = UPPER(pa.player_name)
        OR UPPER(pl.player_name) LIKE '%' || UPPER(SPLIT_PART(pa.player_name, ' ', -1)) || '%'
    )
