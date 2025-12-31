# Team Shot Statistics Models

## Overview

New dbt models for analyzing team-level shot statistics and metrics in NHL games.

## Models Created

### 1. `fact_team_game_stats` (Table)
**Grain:** One row per team per game (2 rows per game for home/away)

**Purpose:** Core fact table for team-level game statistics with focus on shots

**Key Columns:**
- `game_id`, `team_id`: Composite primary key
- `shots_for`, `shots_against`: Shots on goal for/against the team
- `shot_differential`: Net shots (shots_for - shots_against)
- `shooting_pct`: Goals scored / shots on goal
- `save_pct`: Goalie save percentage
- `goals_for`, `goals_against`, `goal_differential`
- Other team stats: `hits`, `giveaways`, `takeaways`, `penalty_minutes`, `pp_goals`, `sh_goals`

**Example Queries:**

```sql
-- Get team shot statistics for a specific game
SELECT 
    team_id,
    shots_for,
    shots_against,
    shot_differential,
    shooting_pct
FROM fact_team_game_stats
WHERE game_id = 2024020001;

-- Compare shots for vs against across all games for a team
SELECT 
    game_date,
    team_id,
    shots_for,
    shots_against,
    result
FROM fact_team_game_stats
WHERE team_id = 10  -- Colorado Avalanche
    AND season = 20242025
ORDER BY game_date;
```

### 2. `team_shot_metrics` (View)
**Grain:** One row per team per game (regular season only)

**Purpose:** Provides rolling averages and season-to-date statistics for shot metrics.

**Key Columns:**
- All columns from `fact_team_game_stats`
- `shots_for_5game_avg`: 5-game rolling average of shots for
- `shots_against_5game_avg`: 5-game rolling average of shots against
- `shots_for_10game_avg`: 10-game rolling average of shots for
- `shots_against_10game_avg`: 10-game rolling average of shots against
- `shots_for_season_avg`: Season-to-date average shots for
- `shots_against_season_avg`: Season-to-date average shots against
- `games_played_in_season`: Number of games played in season (useful for filtering)

**Example Queries:**

```sql
-- Get current performance vs season average for a team
SELECT 
    game_date,
    team_id,
    shots_for,
    shots_for_season_avg,
    shots_for - shots_for_season_avg AS shots_vs_avg
FROM team_shot_metrics
WHERE team_id = 10
ORDER BY game_date DESC
LIMIT 10;

-- Teams with improving shot trends (5-game avg > 10-game avg)
SELECT 
    team_id,
    game_date,
    shots_for_5game_avg,
    shots_for_10game_avg,
    shots_for_5game_avg - shots_for_10game_avg AS trend
FROM team_shot_metrics
WHERE games_played_in_season >= 10  -- At least 10 games played
    AND shots_for_5game_avg > shots_for_10game_avg
ORDER BY trend DESC;

-- Shot differential trends
SELECT 
    team_id,
    AVG(shots_for_5game_avg - shots_against_5game_avg) AS avg_shot_differential_5g
FROM team_shot_metrics
WHERE games_played_in_season >= 20
GROUP BY team_id
ORDER BY avg_shot_differential_5g DESC;
```

## Analytics Use Cases

### 1. Shot Volume Analysis
- Identify teams that allow the fewest shots against
- Monitor shot differential trends over time

### 2. Efficiency Metrics
- Compare shooting percentage across teams
- Analyze save percentage trends
- Identify over/under-performing teams (actual goals vs expected based on shots)

### 3. Rolling Averages
- Spot teams on hot/cold streaks using 5-game averages
- Compare recent performance (5-game) vs longer-term (10-game, season)
- Filter for teams with minimum games played to ensure statistical significance

### 4. Predictive Analysis
- Use shot metrics as predictive features for game outcomes
- Shot differential is often a better predictor than goal differential
- Rolling averages can indicate team momentum

## Building the Models

```bash
# Build all team stat models
dbt run --select fact_team_game_stats team_shot_metrics

# Build with dependencies
dbt run --select +fact_team_game_stats+

# Test the models
dbt test --select fact_team_game_stats
```

## Dependencies

- `bronze_game_boxscore_snapshots`: Raw game data
- `fact_player_game_stats`: Player-level statistics

## Notes

- Shot attempts not yet included (tracking missed shots)
- The `team_shot_metrics` view only includes regular season games (game_type = 2) for cleaner statistical analysis
- Rolling averages use `ROWS BETWEEN X PRECEDING AND CURRENT ROW` to include the current game
- All percentages are stored as 0-100 scale (not 0-1)
