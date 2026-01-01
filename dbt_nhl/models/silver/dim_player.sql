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

WITH player_games AS (
    {%- for combo in combinations %}
    
    -- Extract {{ combo.team }} {{ combo.path }}
    SELECT
        p.value:playerId::INT AS player_id,
        p.value:name.default::STRING AS player_name,
        p.value:position::STRING AS position_code,
        '{{ combo.type }}' AS position_type,
        payload:{{ combo.team }}.id::INT AS team_id,
        payload:{{ combo.team }}.abbrev::STRING AS team_abbrev,
        payload:gameDate::DATE AS game_date
    FROM {{ ref('bronze_game_boxscore_snapshots') }},
    LATERAL FLATTEN(input => payload:playerByGameStats:{{ combo.team }}:{{ combo.path }}) p
    WHERE p.value:playerId IS NOT NULL
    
        {%- if not loop.last %}
    UNION ALL
        {%- endif %}
    {%- endfor %}
),

-- Get most recent game per player to determine current team
latest_player_team AS (
    SELECT
        player_id,
        player_name,
        position_code,
        position_type,
        team_id,
        team_abbrev
    FROM player_games
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id
        ORDER BY game_date DESC, team_id DESC, position_type, position_code
    ) = 1
),

-- Derive current season for headshot URL
current_season AS (
    SELECT
        CAST(
            CASE 
                WHEN MONTH(CURRENT_DATE()) >= 10 
                THEN CONCAT(YEAR(CURRENT_DATE()), YEAR(CURRENT_DATE()) + 1)
                ELSE CONCAT(YEAR(CURRENT_DATE()) - 1, YEAR(CURRENT_DATE()))
            END
            AS INT
        ) AS season_code
)

SELECT
    lpt.player_id,
    lpt.player_name,
    lpt.position_code,      -- Specific position: C, LW, RW, D
    lpt.position_type,      -- Broader grouping: F (forward) or D (defense)
    lpt.team_id AS current_team_id,
    lpt.team_abbrev AS current_team_abbrev,
    -- Player headshot URL (NHL CDN) - format: /mugs/nhl/{season}/{team}/{player_id}.png
    CONCAT('https://assets.nhle.com/mugs/nhl/', cs.season_code, '/', lpt.team_abbrev, '/', lpt.player_id, '.png') AS headshot_url
FROM latest_player_team lpt
CROSS JOIN current_season cs
WHERE lpt.player_id IS NOT NULL
ORDER BY lpt.player_id
