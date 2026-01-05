{{
  config(
    materialized='table',
    tags=['silver', 'bruins']
  )
}}

-- Shot locations for the Bruins' next opponent (offensive shots only)
-- Shows where the opponent shoots FROM - same view as player shot map
-- Automatically updates based on the upcoming schedule

WITH next_opponent AS (
    SELECT opponent_abbrev
    FROM {{ ref('bruins_next_opponent') }}
)

SELECT 
    tsl.*,
    no.opponent_abbrev AS context_label
FROM {{ ref('team_shot_locations') }} tsl
CROSS JOIN next_opponent no
WHERE tsl.team_abbrev = no.opponent_abbrev
  AND tsl.shot_context = 'offense'  -- Only offensive shots (where they shoot from)
