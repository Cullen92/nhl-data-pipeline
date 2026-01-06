{{
  config(
    materialized='table',
    tags=['staging', 'odds', 'mapping']
  )
}}

-- Staging: Player name crosswalk for matching odds API names to NHL API players
-- This handles the common naming differences between betting sites and NHL.
--
-- Common differences:
-- 1. Full name vs shortened: "J.T. Miller" vs "JT Miller"  
-- 2. Accented characters: "Léon Draisaitl" vs "Leon Draisaitl"
-- 3. Suffix handling: "Ryan Suter Jr." vs "Ryan Suter"
-- 4. Formatting: "Connor McDavid" = "Connor Mcdavid" (case)

WITH nhl_players AS (
    -- Get all NHL players with normalized names for matching
    SELECT
        player_id,
        player_name,
        current_team_abbrev,
        position_type,
        -- Create normalized versions for matching
        UPPER(TRIM(player_name)) AS player_name_upper,
        -- Extract last name (last word)
        UPPER(SPLIT_PART(TRIM(player_name), ' ', -1)) AS last_name_upper,
        -- Remove accents using REGEXP_REPLACE (handles unicode better than TRANSLATE)
        UPPER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(player_name), '[ÁÀÂÃÄÅ]', 'A'),
                            '[ÉÈÊË]', 'E'),
                        '[ÍÌÎÏ]', 'I'),
                    '[ÓÒÔÕÖØ]', 'O'),
                '[ÚÙÛÜ]', 'U')
        ) AS player_name_normalized,
        -- First initial + last name pattern
        UPPER(
            CONCAT(
                LEFT(TRIM(player_name), 1), 
                '. ',
                SPLIT_PART(TRIM(player_name), ' ', -1)
            )
        ) AS first_initial_last
    FROM {{ ref('dim_player') }}
),

odds_player_names AS (
    -- Get distinct player names from odds data
    SELECT DISTINCT
        player_name AS odds_player_name,
        UPPER(TRIM(player_name)) AS odds_name_upper,
        -- Extract last name
        UPPER(SPLIT_PART(TRIM(player_name), ' ', -1)) AS odds_last_name,
        -- Normalized (remove periods, extra spaces)
        UPPER(
            REPLACE(REPLACE(TRIM(player_name), '.', ''), '  ', ' ')
        ) AS odds_name_normalized
    FROM {{ ref('stg_odds_player_props') }}
    WHERE player_name IS NOT NULL
),

-- Strategy 1: Exact match (case-insensitive)
exact_matches AS (
    SELECT
        o.odds_player_name,
        n.player_id,
        n.player_name AS nhl_player_name,
        n.current_team_abbrev,
        'exact' AS match_method,
        1.0 AS confidence
    FROM odds_player_names o
    INNER JOIN nhl_players n ON o.odds_name_upper = n.player_name_upper
),

-- Strategy 2: Normalized match (removes accents, etc.)
normalized_matches AS (
    SELECT
        o.odds_player_name,
        n.player_id,
        n.player_name AS nhl_player_name,
        n.current_team_abbrev,
        'normalized' AS match_method,
        0.95 AS confidence
    FROM odds_player_names o
    INNER JOIN nhl_players n 
        ON o.odds_name_normalized = n.player_name_normalized
    WHERE o.odds_player_name NOT IN (SELECT odds_player_name FROM exact_matches)
),

-- Strategy 3: Last name match with first initial
-- Handles "J.T. Miller" -> "JT Miller" type differences
initial_last_matches AS (
    SELECT
        o.odds_player_name,
        n.player_id,
        n.player_name AS nhl_player_name,
        n.current_team_abbrev,
        'initial_last' AS match_method,
        0.85 AS confidence
    FROM odds_player_names o
    INNER JOIN nhl_players n 
        ON o.odds_last_name = n.last_name_upper
        AND LEFT(o.odds_name_upper, 1) = LEFT(n.player_name_upper, 1)
    WHERE o.odds_player_name NOT IN (SELECT odds_player_name FROM exact_matches)
      AND o.odds_player_name NOT IN (SELECT odds_player_name FROM normalized_matches)
    -- Only keep if there's a single match
    QUALIFY COUNT(*) OVER (PARTITION BY o.odds_player_name) = 1
),

-- Combine all matches, preferring higher confidence
all_matches AS (
    SELECT * FROM exact_matches
    UNION ALL
    SELECT * FROM normalized_matches
    UNION ALL
    SELECT * FROM initial_last_matches
),

-- Deduplicate, keeping best match per odds name
final_crosswalk AS (
    SELECT
        odds_player_name,
        player_id AS nhl_player_id,
        nhl_player_name,
        current_team_abbrev,
        match_method,
        confidence
    FROM all_matches
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY odds_player_name
        ORDER BY confidence DESC, nhl_player_name
    ) = 1
)

SELECT * FROM final_crosswalk
