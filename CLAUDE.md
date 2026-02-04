# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NHL Data Analytics Pipeline - A production-grade data platform for NHL game analytics with dual lakehouse architectures (Snowflake + Apache Iceberg). The project implements medallion architecture (bronze/silver/gold) for both paths, featuring automated ingestion from NHL APIs, dimensional modeling, and advanced player statistics analysis.

**Primary Goal:** Generate predictive reports for NHL player props, specifically Shots on Goal (SOG) projections.

## Development Commands

### Environment Setup
```bash
# Initial setup (WSL + venv)
make setup

# Activate virtual environment
source .venv/bin/activate
```

### Testing & Quality
```bash
# Run pytest suite
make test

# Linting (ruff)
make lint
make lint-fix

# Data quality validation (Snowflake Time Travel)
make validate-data
```

### dbt Operations
```bash
# Run all dbt models (requires .env sourced)
make dbt-run

# Run odds-related models only
make dbt-run-odds

# Test odds models
make dbt-test-odds

# Run dbt and export to Google Sheets
make dbt-run-export
```

### MWAA Deployment
```bash
# Build plugins.zip for MWAA
make build-plugins

# Sync DAGs and dbt project to S3
make sync-mwaa

# Full deployment (build + sync)
make deploy-mwaa
```

### Iceberg Operations
```bash
# Load game boxscore data to Iceberg bronze layer
python iceberg/bronze_game_boxscore.py
```

### Data Export
```bash
# Export to Google Sheets (for Tableau Public)
make export-sheets

# Export shot location data for Tableau heatmaps
make export-tableau
make export-tableau-full  # includes raw events
```

## Architecture Overview

### Dual Data Path Strategy

This project maintains **two parallel data paths** to learn both proprietary and open-source lakehouse patterns:

```
                    ┌─────────────────────────────────────┐
                    │   NHL API / Odds API (Sources)      │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │  Airflow (AWS MWAA) Orchestration   │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │     S3 Data Lake (Raw JSON)         │
                    └──────┬────────────────────┬─────────┘
                           │                    │
        ┌──────────────────▼─────┐    ┌────────▼──────────────────┐
        │  SNOWFLAKE PATH         │    │  ICEBERG PATH             │
        │  (Production)           │    │  (Learning/Future)        │
        ├─────────────────────────┤    ├───────────────────────────┤
        │ Bronze: RAW_NHL views   │    │ Bronze: PyIceberg tables  │
        │ Silver: dbt (dim/fact)  │    │ Silver: (planned)         │
        │ Gold: (planned)         │    │ Gold: (planned)           │
        │                         │    │                           │
        │ Query: Snowflake SQL    │    │ Query: DuckDB/Athena      │
        │ Export: Google Sheets   │    │ Export: Streamlit (plan)  │
        │         → Tableau       │    │                           │
        └─────────────────────────┘    └───────────────────────────┘
```

**Snowflake Path (Current Production):**
- Bronze: Immutable raw JSON views with `COPY INTO` metadata extraction
- Silver: dbt-managed dimensional model (sparse dims, denormalized facts)
- Gold: Pre-aggregated analytics (planned)
- Export: Google Sheets → Tableau Public

**Iceberg Path (Future):**
- Bronze: PyIceberg + Glue Catalog + S3 Parquet
- Silver/Gold: TBD (Spark/DuckDB transformations)
- Export: Streamlit dashboards

See [.context/DECISIONS.md#2026-01-17](.context/DECISIONS.md#2026-01-17-adopt-apache-iceberg-for-lakehouse-architecture) for lakehouse rationale.

### Medallion Architecture Layers

**Bronze Layer (Immutable Raw):**
- Snowflake: Views over RAW_NHL external stage tables
- Iceberg: Parquet tables in AWS Glue Catalog
- No transformations, metadata extraction only (game_id, partition_date from S3 paths)
- Critical pattern: Use `[0-9]` instead of `\d` in Snowflake regex (see DECISIONS.md)

**Silver Layer (Cleaned & Modeled):**
- Dimensional model with sparse dimensions (dim_player, dim_team, dim_date)
- Fact tables with denormalized attributes (player_name, team_abbrev in facts)
- Uses `QUALIFY ROW_NUMBER()` for deduplication (most recent snapshot pattern)
- Filters to completed games only (`game_state = 'OFF'`)

**Gold Layer (Aggregated Analytics):**
- Pre-aggregated, ML-ready feature tables
- Rolling averages, matchup analysis

### Key Architectural Decisions

1. **Sparse Dimensions:** Dimensions populate organically from game data rather than pre-loaded reference tables
2. **Denormalized Facts:** Include commonly-queried attributes (names, abbreviations) alongside FKs for query performance
3. **Two-Level Position Hierarchy:** Both `position_code` (C/L/R/D) and `position_type` (F/D) stored
4. **dbt Cloud for Transformation:** Executed via Airflow's `DbtCloudRunJobOperator` (avoids MWAA dependency conflicts)
5. **Jinja for Repetitive SQL:** Loop over team/position combinations to reduce code duplication

See [.context/DECISIONS.md](.context/DECISIONS.md) for complete decision log with context and rationale.

## Source Code Structure

```
src/nhl_pipeline/
├── ingestion/           # API fetchers and S3 uploaders
│   ├── fetch_schedule.py
│   ├── fetch_game_boxscore.py
│   ├── fetch_game_pbp.py
│   ├── fetch_odds_props.py
│   ├── api_utils.py     # Rate limiting, retry logic
│   └── s3_utils.py      # S3 upload helpers
├── export/              # Data export modules
│   ├── sheets_export.py # Google Sheets for Tableau
│   └── tableau_export.py
├── utils/
│   ├── datetime_utils.py
│   ├── time_travel_validator.py  # Snowflake data quality checks
│   └── paths.py
└── config.py            # Settings loader (YAML + env vars + Airflow Variables)

dags/                    # Airflow DAGs (deployed to MWAA)
├── nhl_daily_ingestion_dag.py       # Main daily pipeline
├── nhl_odds_daily_ingestion.py      # Player props
├── nhl_backfill_dag.py              # Historical loads
└── nhl_raw_stats_skater_daily.py

dbt_nhl/                 # dbt Cloud project (synced to S3)
├── models/
│   ├── bronze/          # Raw views
│   ├── silver/          # Dimensional model
│   ├── staging/         # Intermediate transformations
│   └── reporting/       # Bruins-specific dashboards
└── macros/
    └── generate_schema_name.sql  # Custom schema naming

iceberg/                 # PyIceberg scripts
└── bronze_game_boxscore.py  # Iceberg bronze loader

config/
├── settings.yml         # Pipeline configuration
└── iceberg.yml          # Iceberg/Glue/S3 configuration
```

## Configuration & Environment

### Environment Variables (.env)

Required for local development and MWAA:

```bash
# Snowflake
SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
SNOWFLAKE_DATABASE=NHL
SNOWFLAKE_WAREHOUSE=NHL_WH
SNOWFLAKE_SCHEMA=STAGING_SILVER
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Google Sheets (for Tableau export)
GOOGLE_SHEETS_CREDENTIALS=<path_to_json>
GOOGLE_SHEET_ID=<sheet_id>

# The Odds API
ODDS_API_KEY=<api_key>
```

### Configuration Priority (config.py)

Settings are loaded in this order (later takes precedence):
1. `config/settings.yml`
2. Airflow Variables (MWAA)
3. Environment variables (both UPPERCASE and lowercase for MWAA compatibility)

**MWAA-specific:** MWAA forces env vars to lowercase (e.g., `s3_bucket` not `S3_BUCKET`). The config loader checks both cases.

### MWAA S3 Bucket Structure

```
s3://mwaa-bucket-nhl-cullenm-dev/
├── dags/                          # Airflow DAGs
│   └── dbt_nhl/                   # dbt project (synced here)
├── plugins.zip                    # nhl_pipeline package
├── requirements/
│   └── requirements-mwaa.txt      # Airflow dependencies
└── config/
    └── google-sheets-credentials.json  # Not in git!
```

## CI/CD & Workflows

### GitHub Actions Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| CI | Push/PR to `main`, `develop` | Linting (ruff) + pytest (soft-failing) |
| Deploy to MWAA | Push to `main` | Syncs DAGs, plugins, dbt to S3 |
| dbt Docs | Push to `main` (dbt changes) | Deploys docs to GitHub Pages |
| Data Quality | After MWAA deploy + every 4 hours | Snowflake Time Travel validation |

### Branching Strategy
- Feature branches → `develop` → `main` (production)
- All changes require PR with passing CI

## dbt Conventions

### Schema Naming
Custom `generate_schema_name` macro produces clean names:
- Bronze models → `STAGING_BRONZE`
- Silver models → `STAGING_SILVER`
- Reporting models → `STAGING_REPORTING`

### Model Organization
- Bronze: 1 model per source (schedule, boxscore, pbp, odds)
- Silver: Dimensional model (3 dims, 5 facts)
- Reporting: Team-specific views (currently Bruins-only due to Tableau Public limits)

### Testing Strategy
- 10+ dbt tests on bronze layer (uniqueness, not_null, accepted_values)
- 7 relationship tests validating FK integrity across dims/facts
- All 54 tests currently passing

### Tags
- `tag:odds` — Models related to player prop betting lines
- Use for selective runs: `dbt run --select tag:odds+`

## Data Quality Patterns

### Snowflake Time Travel Validation
Script: `src/nhl_pipeline/utils/time_travel_validator.py`

Compares current table state vs. historical snapshot (default 1 hour ago):
- Row count drift detection
- NULL value percentage changes
- Alerts on significant deviations

Run via: `make validate-data`

### Idempotency Patterns
- **Most Recent Snapshot:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY partition_date DESC) = 1`
- **Deduplication:** All silver models use this pattern to handle reprocessing
- **Completed Games Only:** Filter `game_state = 'OFF'` in silver, not bronze

### Metadata Extraction from S3 Paths
Critical pattern for bronze layer:

```sql
-- ✅ CORRECT (use [0-9] not \d)
game_id AS TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'game_id=([0-9]+)', 1, 1, 'e'))
partition_date AS TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e'))

-- ❌ WRONG (Python \d escape causes issues)
'game_id=(\\d+)'  -- Don't use this!
```

See [.context/DECISIONS.md#2026-01-09](.context/DECISIONS.md#2026-01-09-snowflake-copy-into-regex-fix) for details.

## Airflow DAG Patterns

### Main DAG: nhl_daily_ingestion
1. Fetch schedule (current week + previous week for rollover games)
2. Extract completed game IDs (`game_state = 'OFF'`)
3. Fetch boxscore + PBP for each game (rate limited: 0.25s between calls)
4. Upload to S3 with metadata in path (`s3://.../game_id=XXX/date=YYYY-MM-DD/...`)
5. COPY INTO Snowflake RAW tables
6. Trigger dbt Cloud job via `DbtCloudRunJobOperator`
7. Export to Google Sheets for Tableau

### Rate Limiting
Configurable via env var: `NHL_DAILY_SLEEP_S=0.25` (default)

### Error Handling
- Retry: 1 attempt with 5-minute delay
- Logs available in MWAA CloudWatch

## Iceberg Specifics

### Configuration
File: `config/iceberg.yml`

```yaml
catalog:
  type: glue
  region: us-east-2
  warehouse: s3://nhl-data-pipeline-cullenm-dev/iceberg

tables:
  bronze:
    game_boxscore:
      partition_spec:
        - field: partition_date
          transform: identity
```

### PyIceberg Usage Pattern

```python
from pyiceberg.catalog import load_catalog
import yaml

# Load config
with open("config/iceberg.yml") as f:
    config = yaml.safe_load(f)

# Initialize catalog
catalog = load_catalog("glue", **config["catalog"])

# Access table
table = catalog.load_table("bronze.game_boxscore")

# Append data
table.append(df)
```

### State Management
Incremental load state tracked in:
- S3: `s3://nhl-data-pipeline-cullenm-dev/iceberg/_state`
- Local: `.iceberg_state/` (gitignored)

### Bronze Layer Patterns (Phase 1 Complete)

**Key Design Principles:**
1. **Temporal Snapshots are Intentional**
   - Bronze contains ALL snapshots of a game/event over time (e.g., scheduled → in-progress → final)
   - "Duplicates" are correct behavior for immutable landing zone
   - Silver layer deduplicates using `QUALIFY ROW_NUMBER() ... ORDER BY extracted_at DESC`

2. **Nullable Fields for Optional Data**
   - Bronze accepts data as-is without enforcing business rules
   - Example: `game_date` nullable in odds (23.68% nulls for futures markets)
   - Silver layer applies business logic filters

3. **Schema Requirements**
   - Use `LongType()` (int64) for NHL game IDs (10-digit integers like 2025020726)
   - PyIceberg 0.10.0+ required for partitioned table writes
   - Partition by `partition_date` (identity transform) for time-based queries

**Current Bronze Tables:**
```
bronze.game_boxscore:      2,141 rows (1,269 unique games)
bronze.odds_player_props:  2,483 rows (player_shots_on_goal market)
Date range:                2024-10-04 to 2026-01-17
```

### Validation

**Script:** `query/validate_bronze.py`

Validates bronze tables using PyIceberg + DuckDB:
- Row counts match S3 file counts
- Date ranges cover expected periods
- Duplicate analysis (temporal snapshots)
- Null percentage checks
- Sample data inspection

**Run validation:**
```bash
python query/validate_bronze.py
```

**Expected output:**
- Total rows per table
- Unique game/event IDs vs total rows (shows temporal snapshots)
- Date range coverage
- Market breakdown (for odds)
- Null game_date percentage
- Sample records with payload sizes

## Testing

### Pytest Structure
Tests located in `tests/` directory. Current coverage:
- API fetch logic
- S3 upload utilities
- Date parsing helpers

Run via: `make test` or `pytest -q`

### Local Testing Without Airflow
The `nhl_pipeline` package is designed to work standalone:
- Import functions directly: `from nhl_pipeline.ingestion.fetch_schedule import fetch_schedule`
- No Airflow dependency for library code (only DAG files)
- Config gracefully handles missing Airflow (`config.py` catches ImportError)

## Common Gotchas

1. **MWAA Environment Variables:** Use lowercase (e.g., `s3_bucket`) not uppercase
2. **dbt Cloud vs Local dbt:** DAGs use dbt Cloud jobs, not local dbt CLI
3. **Google Sheets Credentials:** Stored in S3, not Airflow Variables (avoids JSON escaping issues)
4. **Regex in COPY INTO:** Always use `[0-9]` character classes, never `\d` shorthand
5. **Virtual Environment:** Always `source .venv/bin/activate` before running local commands
6. **Tableau Public Limits:** Bruins-only models due to 48K+ row Google Sheets limitations
7. **Iceberg Bronze "Duplicates":** Temporal snapshots are intentional, not errors. Don't deduplicate in bronze.
8. **PyIceberg Version:** Use 0.10.0+ for partitioned table writes. Earlier versions fail with partitioned tables.
9. **NHL Game ID Data Type:** Use `LongType()` (int64) not `IntegerType()` (int32) for 10-digit game IDs
10. **Iceberg Schema Changes:** Schema mismatches require table recreation. Drop table + delete S3 data before changing types.

## Future Roadmap

- Complete Iceberg silver/gold layers with Spark transformations
- Migrate from Tableau Public to Streamlit for full league coverage
- Implement ML models for SOG predictions
- Add streaming data ingestion for live games
- Warehouse-native A/B testing with Eppo (position-based vs location-based defensive models)
- Separate Dev/Stage/Prod environments with formal promotion strategy

## Additional Documentation

- [README.md](README.md) — Full project documentation
- [.context/DECISIONS.md](.context/DECISIONS.md) — Chronological decision log with rationale
- [.context/ARCHITECTURE.md](.context/ARCHITECTURE.md) — Detailed architecture design (if exists)
- [docs/time_travel_validation.md](docs/time_travel_validation.md) — Time Travel validation details
- [dbt Docs](https://nhl-data-pipeline.github.io/nhl-data-pipeline/) — Interactive lineage graph (GitHub Pages)
