{{
  config(
    materialized='table'
  )
}}

-- Silver layer: Player game statistics fact table
-- Grain: One row per player per game
-- Contains individual player performance metrics with denormalized attributes

{%- set stat_fields = [
    {'json': 'goals', 'alias': 'goals', 'type': 'INT'},
    {'json': 'assists', 'alias': 'assists', 'type': 'INT'},
    {'json': 'points', 'alias': 'points', 'type': 'INT'},
    {'json': 'plusMinus', 'alias': 'plus_minus', 'type': 'INT'},
    {'json': 'sog', 'alias': 'shots', 'type': 'INT'},
    {'json': 'pim', 'alias': 'penalty_minutes', 'type': 'INT'},
    {'json': 'powerPlayGoals', 'alias': 'pp_goals', 'type': 'INT'},
    {'json': 'shorthandedGoals', 'alias': 'sh_goals', 'type': 'INT'},
    {'json': 'hits', 'alias': 'hits', 'type': 'INT'},
    {'json': 'blockedShots', 'alias': 'blocked_shots', 'type': 'INT'},
    {'json': 'giveaways', 'alias': 'giveaways', 'type': 'INT'},
    {'json': 'takeaways', 'alias': 'takeaways', 'type': 'INT'},
    {'json': 'faceoffWinningPctg', 'alias': 'faceoff_win_pct', 'type': 'FLOAT'},
    {'json': 'toi', 'alias': 'time_on_ice', 'type': 'STRING'},
    {'json': 'shifts', 'alias': 'shifts', 'type': 'INT'}
] %}

{%- set combinations = [
    {'team_side': 'home', 'team_path': 'homeTeam', 'type': 'F', 'path': 'forwards'},
    {'team_side': 'home', 'team_path': 'homeTeam', 'type': 'D', 'path': 'defense'},
    {'team_side': 'away', 'team_path': 'awayTeam', 'type': 'F', 'path': 'forwards'},
    {'team_side': 'away', 'team_path': 'awayTeam', 'type': 'D', 'path': 'defense'}
] %}

WITH latest_snapshots AS (
    -- Get most recent snapshot for each game
    SELECT
        payload:id::INT AS game_id,
        payload:gameDate::DATE AS game_date,
        payload:homeTeam.id::INT AS home_team_id,
        payload:homeTeam.abbrev::STRING AS home_team_abbrev,
        payload:awayTeam.id::INT AS away_team_id,
        payload:awayTeam.abbrev::STRING AS away_team_abbrev,
        payload:gameState::STRING AS game_state,
        payload,
        partition_date
    FROM {{ ref('bronze_game_boxscore_snapshots') }}
    WHERE payload:gameState = 'OFF'  -- Only completed games
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:id ORDER BY partition_date DESC, s3_key DESC) = 1
),

player_stats AS (
    {%- for combo in combinations %}
    
    -- Extract {{ combo.team_side }} team {{ combo.path }}
    SELECT
        ls.game_id,
        ls.game_date,
        p.value:playerId::INT AS player_id,
        
        -- Team context
        CASE 
            WHEN '{{ combo.team_side }}' = 'home' THEN ls.home_team_id
            ELSE ls.away_team_id
        END AS team_id,
        CASE 
            WHEN '{{ combo.team_side }}' = 'home' THEN ls.away_team_id
            ELSE ls.home_team_id
        END AS opponent_team_id,
        CASE 
            WHEN '{{ combo.team_side }}' = 'home' THEN ls.away_team_abbrev
            ELSE ls.home_team_abbrev
        END AS opponent_team_abbrev,
        '{{ combo.team_side }}' AS home_away,
        
        -- Position
        p.value:position::STRING AS position_code,
        '{{ combo.type }}' AS position_type,
        
        -- Player stats (using Jinja to reduce repetition in the stat fields column list)
        {%- for stat in stat_fields %}
        p.value:{{ stat.json }}::{{ stat.type }} AS {{ stat.alias }}{{ ',' if not loop.last else '' }}
        {%- endfor %}
        
    FROM latest_snapshots ls,
    LATERAL FLATTEN(input => ls.payload:playerByGameStats:{{ combo.team_path }}:{{ combo.path }}) p
    WHERE p.value:playerId IS NOT NULL
    
        {%- if not loop.last %}
    UNION ALL
        {%- endif %}
    {%- endfor %}
)

SELECT
    -- Composite Primary Key
    ps.game_id,
    ps.player_id,
    
    -- Foreign Keys
    ps.game_date AS date_key,
    ps.team_id,
    ps.opponent_team_id,
    
    -- Context (denormalized for query performance)
    p.player_name,
    t.team_abbrev,
    ps.opponent_team_abbrev,
    ps.home_away,
    ps.position_code,
    ps.position_type,
    
    -- Core counting stats
    ps.goals,
    ps.assists,
    ps.points,
    ps.plus_minus,
    ps.shots,
    ps.penalty_minutes,
    
    -- Special teams
    ps.pp_goals,
    ps.sh_goals,
    
    -- Physical/defensive stats
    ps.hits,
    ps.blocked_shots,
    ps.giveaways,
    ps.takeaways,
    
    -- Advanced stats
    ps.faceoff_win_pct,
    ps.time_on_ice,
    ps.shifts

FROM player_stats ps
-- Join dimensions to denormalize attributes
LEFT JOIN {{ ref('dim_player') }} p ON ps.player_id = p.player_id
LEFT JOIN {{ ref('dim_team') }} t ON ps.team_id = t.team_id
WHERE ps.game_id IS NOT NULL
  AND ps.player_id IS NOT NULL
ORDER BY ps.game_date DESC, ps.game_id, ps.player_id
