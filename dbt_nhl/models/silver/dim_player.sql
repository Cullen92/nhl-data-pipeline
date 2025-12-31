{{
  config(
    materialized='table'
  )
}}

-- Silver layer: Player dimension (sparse dimension)
-- One row per NHL player
-- Players are relatively static, but we'll see new players (rookies, callups) throughout season

{%- set combinations = [
    {'team': 'homeTeam', 'type': 'F', 'path': 'forwards'},
    {'team': 'homeTeam', 'type': 'D', 'path': 'defense'},
    {'team': 'awayTeam', 'type': 'F', 'path': 'forwards'},
    {'team': 'awayTeam', 'type': 'D', 'path': 'defense'}
] %}

WITH player_data AS (
    {%- for combo in combinations %}
    
    -- Extract {{ combo.team }} {{ combo.path }}
    SELECT DISTINCT
        p.value:playerId::INT AS player_id,
        p.value:name.default::STRING AS player_name,
        p.value:position::STRING AS position_code,
        '{{ combo.type }}' AS position_type
    FROM {{ ref('bronze_game_boxscore_snapshots') }},
    LATERAL FLATTEN(input => payload:playerByGameStats:{{ combo.team }}:{{ combo.path }}) p
    WHERE p.value:playerId IS NOT NULL
    
        {%- if not loop.last %}
    UNION
        {%- endif %}
    {%- endfor %}
)

SELECT
    player_id,
    player_name,
    position_code,      -- Specific position: C, LW, RW, D
    position_type,      -- Broader grouping: F (forward) or D (defense)
    -- TODO: Add team_id, jersey_number, shoots (L/R), height, weight when available
    NULL AS current_team_id
FROM player_data
WHERE player_id IS NOT NULL
ORDER BY player_id
