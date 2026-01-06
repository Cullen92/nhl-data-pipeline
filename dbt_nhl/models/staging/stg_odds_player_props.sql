{{
  config(
    materialized='view',
    tags=['staging', 'odds']
  )
}}

-- Staging: Parse player prop odds from raw JSON
-- Extracts individual player lines from nested bookmaker data
-- Takes the first available line per player (typically DraftKings/FanDuel)

WITH raw_props AS (
    SELECT
        payload,
        game_date,
        event_id,
        market
    FROM {{ ref('bronze_odds_player_props') }}
),

-- Extract top-level fields
parsed AS (
    SELECT
        game_date,
        event_id,
        market,
        payload:extracted_at::TIMESTAMP_NTZ AS extracted_at,
        payload:home_team::STRING AS home_team,
        payload:away_team::STRING AS away_team,
        payload:data:commence_time::TIMESTAMP_NTZ AS commence_time,
        payload:data:bookmakers AS bookmakers
    FROM raw_props
),

-- Flatten bookmakers and markets
flattened_bookmakers AS (
    SELECT
        p.game_date,
        p.event_id,
        p.market,
        p.extracted_at,
        p.home_team,
        p.away_team,
        p.commence_time,
        bm.value:key::STRING AS bookmaker_key,
        bm.value:title::STRING AS bookmaker_name,
        bm.value:markets AS markets
    FROM parsed p,
    LATERAL FLATTEN(input => p.bookmakers) bm
),

-- Flatten markets
flattened_markets AS (
    SELECT
        fb.game_date,
        fb.event_id,
        fb.market,
        fb.extracted_at,
        fb.home_team,
        fb.away_team,
        fb.commence_time,
        fb.bookmaker_key,
        fb.bookmaker_name,
        mkt.value:key::STRING AS market_key,
        mkt.value:last_update::TIMESTAMP_NTZ AS line_last_update,
        mkt.value:outcomes AS outcomes
    FROM flattened_bookmakers fb,
    LATERAL FLATTEN(input => fb.markets) mkt
),

-- Flatten outcomes to get individual player lines
flattened_outcomes AS (
    SELECT
        fm.game_date,
        fm.event_id,
        fm.market,
        fm.extracted_at,
        fm.home_team,
        fm.away_team,
        fm.commence_time,
        fm.bookmaker_key,
        fm.bookmaker_name,
        fm.market_key,
        fm.line_last_update,
        outcome.value:name::STRING AS bet_type,  -- "Over" or "Under"
        outcome.value:description::STRING AS player_name,
        outcome.value:point::FLOAT AS line_value,
        outcome.value:price::INT AS odds_american
    FROM flattened_markets fm,
    LATERAL FLATTEN(input => fm.outcomes) outcome
)

SELECT
    game_date,
    event_id,
    market,
    extracted_at,
    home_team,
    away_team,
    commence_time,
    bookmaker_key,
    bookmaker_name,
    market_key,
    line_last_update,
    bet_type,
    player_name,
    line_value,
    odds_american
FROM flattened_outcomes
WHERE player_name IS NOT NULL  -- Filter out any malformed data
