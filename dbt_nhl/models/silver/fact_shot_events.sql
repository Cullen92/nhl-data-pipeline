{{
  config(
    materialized='table',
    tags=['silver', 'shots']
  )
}}

-- Silver layer: Shot events with coordinates
-- Grain: One row per shot event (shots on goal, missed shots, goals)
-- Contains shot coordinates for heatmap/location analysis
-- Links to shooter (player) and both teams

WITH pbp_events AS (
    SELECT
        payload:id::INT AS game_id,
        payload:gameDate::DATE AS game_date,
        payload:season::INT AS season,
        payload:gameType::INT AS game_type,
        payload:homeTeam.id::INT AS home_team_id,
        payload:homeTeam.abbrev::STRING AS home_team_abbrev,
        payload:awayTeam.id::INT AS away_team_id,
        payload:awayTeam.abbrev::STRING AS away_team_abbrev,
        payload
    FROM {{ ref('bronze_game_pbp_snapshots') }}
    -- Get most recent snapshot per game
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:id 
        ORDER BY partition_date DESC, s3_key DESC
    ) = 1
),

-- Flatten play events and filter to shot-related events
shot_events AS (
    SELECT
        e.game_id,
        e.game_date,
        e.season,
        e.game_type,
        e.home_team_id,
        e.home_team_abbrev,
        e.away_team_id,
        e.away_team_abbrev,
        
        -- Event details
        play.value:eventId::INT AS event_id,
        play.value:typeDescKey::STRING AS event_type,
        play.value:periodDescriptor.number::INT AS period,
        play.value:periodDescriptor.periodType::STRING AS period_type,
        play.value:timeInPeriod::STRING AS time_in_period,
        play.value:timeRemaining::STRING AS time_remaining,
        
        -- Shot coordinates (origin is center ice, x/y in feet)
        play.value:details.xCoord::FLOAT AS x_coord,
        play.value:details.yCoord::FLOAT AS y_coord,
        
        -- Shooter info (API uses different field names: shootingPlayerId for shots, scoringPlayerId for goals)
        COALESCE(
            play.value:details.shootingPlayerId::INT,
            play.value:details.scoringPlayerId::INT
        ) AS shooter_player_id,
        
        -- Goalie info (for shots on goal)
        play.value:details.goalieInNetId::INT AS goalie_id,
        
        -- Shot type/result details
        play.value:details.shotType::STRING AS shot_type,
        play.value:details.reason::STRING AS miss_reason,
        
        -- Team context
        play.value:details.eventOwnerTeamId::INT AS shooting_team_id,
        
        -- For goals: additional details
        play.value:details.assist1PlayerId::INT AS assist1_player_id,
        play.value:details.assist2PlayerId::INT AS assist2_player_id,
        play.value:details.homeScore::INT AS home_score_after,
        play.value:details.awayScore::INT AS away_score_after
        
    FROM pbp_events e,
    LATERAL FLATTEN(input => e.payload:plays) play
    WHERE play.value:typeDescKey::STRING IN (
        'shot-on-goal',    -- Shot saved by goalie
        'goal',            -- Shot that scored
        'missed-shot'      -- Shot that missed the net
    )
),

-- Add shooter position from dim_player
enriched AS (
    SELECT
        se.*,
        
        -- Derive defending team
        CASE 
            WHEN se.shooting_team_id = se.home_team_id THEN se.away_team_id
            ELSE se.home_team_id
        END AS defending_team_id,
        
        -- Shooter position info
        p.player_name AS shooter_name,
        p.position_code AS shooter_position_code,
        p.position_type AS shooter_position_type,
        
        -- Shot classification
        CASE 
            WHEN se.event_type = 'goal' THEN 'goal'
            WHEN se.event_type = 'shot-on-goal' THEN 'saved'
            WHEN se.event_type = 'missed-shot' THEN 'missed'
        END AS shot_result,
        
        -- Is this a goal?
        CASE WHEN se.event_type = 'goal' THEN 1 ELSE 0 END AS is_goal,
        
        -- Home/away context
        CASE 
            WHEN se.shooting_team_id = se.home_team_id THEN 'home'
            ELSE 'away'
        END AS shooter_home_away
        
    FROM shot_events se
    LEFT JOIN {{ ref('dim_player') }} p 
        ON se.shooter_player_id = p.player_id
)

SELECT
    -- Primary key
    game_id,
    event_id,
    
    -- Game context
    game_date,
    season,
    game_type,
    period,
    period_type,
    time_in_period,
    time_remaining,
    
    -- Shot location (for heatmaps)
    x_coord,
    y_coord,
    
    -- Shooter info
    shooter_player_id,
    shooter_name,
    shooter_position_code,
    shooter_position_type,
    shooting_team_id,
    shooter_home_away,
    
    -- Defending team (team being shot against)
    defending_team_id,
    
    -- Goalie
    goalie_id,
    
    -- Shot details
    event_type,
    shot_result,
    shot_type,
    miss_reason,
    is_goal,
    
    -- Goal assists (NULL for non-goals)
    assist1_player_id,
    assist2_player_id,
    
    -- Score after (for goals)
    home_score_after,
    away_score_after,
    
    -- Team abbrevs for convenience
    home_team_id,
    home_team_abbrev,
    away_team_id,
    away_team_abbrev

FROM enriched
WHERE game_type = 2  -- Regular season only (remove to include playoffs/preseason)
ORDER BY game_date DESC, game_id, period, time_in_period
