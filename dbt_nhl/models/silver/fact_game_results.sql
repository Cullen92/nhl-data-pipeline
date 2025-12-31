{{
  config(
    materialized='table'
  )
}}

-- Silver layer: Game results fact table
-- Grain: One row per game
-- Contains game-level outcomes and scores

WITH latest_snapshots AS (
    -- Get the most recent snapshot for each game
    -- (handles cases where we may have multiple snapshots due to data updates)
    SELECT
        payload:id::INT AS game_id,
        payload:season::INT AS season,
        payload:gameType::INT AS game_type,
        payload:gameDate::DATE AS game_date,
        payload:gameState::STRING AS game_state,
        payload:venue.default::STRING AS venue_name,
        
        -- Home team
        payload:homeTeam.id::INT AS home_team_id,
        payload:homeTeam.score::INT AS home_score,
        
        -- Away team
        payload:awayTeam.id::INT AS away_team_id,
        payload:awayTeam.score::INT AS away_score,
        
        -- Game details
        payload:periodDescriptor.number::INT AS periods_played,
        
        -- Metadata
        partition_date,
        s3_key
    FROM {{ ref('bronze_game_boxscore_snapshots') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC, s3_key DESC) = 1
)

SELECT
    -- Primary Key
    game_id,
    
    -- Foreign Keys (dimension references)
    game_date AS date_key,
    home_team_id,
    away_team_id,
    
    -- Game Classification
    season,
    game_type,  -- 2 = Regular Season, 3 = Playoffs
    game_state, -- OFF = Final, LIVE = In Progress, FUT = Future
    
    -- Measurable Facts
    home_score,
    away_score,
    periods_played,  -- Usually 3, can be 4+ for OT/SO
    
    -- Derived Facts
    CASE 
        WHEN home_score > away_score THEN home_team_id
        WHEN away_score > home_score THEN away_team_id
        ELSE NULL  -- Tie (shouldn't happen in modern NHL)
    END AS winning_team_id,
    
    CASE 
        WHEN periods_played > 3 THEN TRUE
        ELSE FALSE
    END AS went_to_overtime,
    
    ABS(home_score - away_score) AS goal_differential,
    home_score + away_score AS total_goals,
    
    -- Denormalized attributes (for query convenience)
    venue_name,
    
    -- Audit fields
    partition_date AS source_partition_date,
    s3_key AS source_s3_key

FROM latest_snapshots
WHERE game_id IS NOT NULL
  AND game_state = 'OFF'  -- Only include completed games
ORDER BY game_date DESC, game_id
