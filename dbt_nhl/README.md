# NHL Analytics dbt Project

This dbt project transforms raw NHL API data into analytics-ready tables.

## Project Structure

```
dbt_nhl/
├── models/
│   ├── staging/        # Clean, typed views over raw data
│   │   ├── stg_games.sql
│   │   ├── stg_player_game_stats.sql
│   │   └── schema.yml
│   └── analytics/      # Business logic, aggregations, facts
│       └── fact_player_game_stats.sql
├── dbt_project.yml     # Project configuration
└── profiles.yml        # Connection configuration
```

## Setup

1. **Install dbt-snowflake:**
   ```bash
   pip install dbt-snowflake
   ```

2. **Set environment variables:**
   ```bash
   export SNOWFLAKE_USER=your_username
   export SNOWFLAKE_PASSWORD=your_password
   ```

3. **Test connection:**
   ```bash
   cd dbt_nhl
   dbt debug
   ```

4. **Run models:**
   ```bash
   dbt run
   ```

5. **Run tests:**
   ```bash
   dbt test
   ```

## Layer Architecture

### RAW_NHL (Bronze)
- `SCHEDULE_SNAPSHOTS` - Raw schedule API responses
- `GAME_BOXSCORE_SNAPSHOTS` - Raw boxscore JSON
- `GAME_PBP_SNAPSHOTS` - Raw play-by-play JSON

### STAGING (Silver)
- `stg_games` - Cleaned game information
- `stg_player_game_stats` - Player statistics per game
- Future: `stg_events`, `stg_teams`, etc.

### ANALYTICS (Gold)
- `fact_player_game_stats` - Player game stats with context
- Future: `dim_players`, `dim_teams`, aggregated metrics

## Development Workflow

1. **Make changes** to models in `models/`
2. **Test locally:**
   ```bash
   dbt run --select model_name
   dbt test --select model_name
   ```
3. **Document changes** in `schema.yml`
4. **Commit to git**
5. **Run in production** (via Airflow or dbt Cloud)

## Integration with Airflow

You can trigger dbt runs from Airflow using the BashOperator or dbt provider:

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt_nhl && dbt run',
)
```

## Next Steps

- [ ] Add incremental models for large tables
- [ ] Add data quality tests
- [ ] Build dimension tables (teams, players)
- [ ] Add macros for common transformations
- [ ] Set up dbt docs (`dbt docs generate && dbt docs serve`)
