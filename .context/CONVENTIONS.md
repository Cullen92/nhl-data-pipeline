# Coding Conventions & Patterns

> Standards and patterns used throughout the codebase. Reference this when contributing or reviewing code.

## Python

### Style

- **Linter:** `ruff` (configured in `pyproject.toml`)
- **Formatter:** Follow ruff defaults
- **Type Hints:** Preferred for public functions

### Project Structure

```
src/nhl_pipeline/
├── __init__.py
├── config.py           # Configuration loading
├── ingestion/          # API fetching & S3 uploads
├── processing/         # Data transformations
└── utils/              # Shared utilities
```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Modules | `snake_case` | `fetch_schedule.py` |
| Functions | `snake_case` | `get_game_boxscore()` |
| Classes | `PascalCase` | `ScheduleFetcher` |
| Constants | `UPPER_SNAKE` | `DEFAULT_SEASON` |

### Import Order

1. Standard library
2. Third-party packages
3. Local imports (`from nhl_pipeline...`)

## SQL / dbt

### Model Naming

| Layer | Prefix | Example |
|-------|--------|---------|
| Staging | `stg_` | `stg_games.sql` |
| Intermediate | `int_` | `int_player_daily.sql` |
| Facts | `fact_` | `fact_player_game_stats.sql` |
| Dimensions | `dim_` | `dim_players.sql` |

### Style

- Keywords: `UPPERCASE`
- Identifiers: `snake_case`
- CTEs preferred over subqueries
- One column per line in SELECT

## Airflow DAGs

### Naming

- DAG files: `nhl_<purpose>_dag.py`
- DAG IDs: Match filename without `.py`

### Patterns

- Use `@task` decorator for simple Python tasks
- Idempotent tasks (safe to re-run)
- Explicit `start_date` and `catchup=False` unless backfilling

## Git

### Branch Naming

- `<name>/<short-description>` (Example: cullenm/AddContextTracking)

### Commit Messages

- Present tense: "Add schedule fetcher" not "Added"
- Reference issues when applicable: "Fix null handling (#42)"

## Testing

- Test files: `test_<module>.py`
- Use `pytest` fixtures for shared setup
- Mock external APIs and S3 in unit tests
