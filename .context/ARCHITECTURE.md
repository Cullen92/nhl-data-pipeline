# NHL Data Pipeline — Architecture

> This document provides LLM agents and collaborators with a deep understanding of the project's design, technology choices, and data flows.

## Project Purpose

A data engineering pipeline to ingest, process, and analyze NHL game data. The short-term goal is generating **Shots on Goal (SOG)** projections for player props analysis.

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Language | Python 3.x, SQL | Core development |
| Dev Environment | VS Code + WSL (Ubuntu) | Local development |
| Orchestration | Apache Airflow (MWAA) | Workflow scheduling |
| Cloud | AWS (S3, IAM, MWAA) | Infrastructure |
| Data Warehouse | Snowflake | Analytical storage |
| Transformation | dbt | Data modeling |
| Processing | PySpark | Planned for heavy transforms |
| Visualization | Looker/Tableau | Planned dashboards |

## Data Flow Architecture

```
┌─────────────────┐
│   NHL Public    │
│      APIs       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Python Ingestion │  ← src/nhl_pipeline/ingestion/
│  (Airflow DAGs)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AWS S3        │  ← Raw JSON storage (Data Lake)
│   (Raw Layer)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowflake     │  ← Staged & transformed data
│   (Warehouse)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   dbt Models    │  ← dbt_nhl/models/
│   (Transform)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Analytics     │  ← Dashboards, ML (future)
└─────────────────┘
```

## Key Components

### Ingestion Layer (`src/nhl_pipeline/ingestion/`)

| Module | Responsibility |
|--------|----------------|
| `fetch_schedule.py` | Daily game schedules |
| `fetch_game_boxscore.py` | Detailed boxscore data |
| `fetch_game_pbp.py` | Play-by-play event data |
| `fetch_stats_skater_reports.py` | Individual skater statistics |
| `s3_utils.py` | S3 upload/download operations |
| `api_utils.py` | NHL API client utilities |

### Airflow DAGs (`dags/`)

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `nhl_daily_ingestion_dag` | Daily | Fetch latest game data |
| `nhl_backfill_dag` | Manual | Historical data loading |
| `nhl_raw_stats_skater_daily` | Daily | Skater statistics pipeline |

### dbt Models (`dbt_nhl/models/`)

| Layer | Models | Purpose |
|-------|--------|---------|
| Staging | `stg_games`, `stg_player_game_stats` | Clean/normalize raw data |
| Analytics | `fact_player_game_stats` | Aggregated player performance |

## Environment Strategy

- **Current:** Development environment
- **Planned:** Dev → Stage → Prod promotion pipeline

## S3 Bucket Structure

```
s3://nhl-data-lake/
├── raw/
│   ├── schedule/
│   ├── boxscores/
│   ├── play_by_play/
│   └── skater_reports/
└── processed/  (future)
```

## Quality & Validation

- **Time Travel Validation:** Uses Snowflake Time Travel to detect unexpected data changes
- **CI/CD:** GitHub Actions for linting (`ruff`) and testing (`pytest`)
- **Data Validation Config:** `config/data_validation.yml`

## Future Roadmap

1. Expand to Goals, Assists, TOI analysis
2. Full CI/CD with Dev/Prod separation
3. PySpark processing for heavy transforms
4. ML/AI predictive modeling
5. Live game streaming data