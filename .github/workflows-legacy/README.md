# Legacy Workflows (Disabled)

This directory contains GitHub Actions workflows from the legacy Snowflake/MWAA data path. These workflows are preserved for reference but are **not executed** by GitHub Actions (workflows must be in `.github/workflows/` to run).

## Disabled Workflows

### data-validation.yml
- **Purpose:** Snowflake Time Travel data quality validation
- **Why disabled:** Project migrated from Snowflake to Apache Iceberg
- **Replacement:** TBD - Iceberg-native validation approach

### deploy.yml
- **Purpose:** Deploy DAGs, plugins, and dbt project to AWS MWAA
- **Why disabled:** Project migrated from Airflow orchestration to direct Python execution
- **Replacement:** Direct execution of ingestion scripts + potential future orchestration

### dbt-docs.yml
- **Purpose:** Generate and deploy dbt documentation to GitHub Pages
- **Why disabled:** Project migrated from dbt (Snowflake) to Iceberg transformations
- **Replacement:** TBD - Iceberg transformation documentation approach

## Migration Context

As of 2026-02, the project is transitioning from:
- **Legacy (Snowflake Path):** Airflow (MWAA) → Snowflake → dbt → Tableau Public
- **New (Iceberg Path):** Python ingestion → Apache Iceberg → DuckDB/Streamlit

The legacy MWAA environment is still running for the Snowflake path, but new development focuses on the Iceberg architecture. See `/requirements.txt` for the updated dependency set.

## Reactivating Workflows

If you need to reactivate any of these workflows:
1. Move the workflow file from `.github/workflows-legacy/` to `.github/workflows/`
2. Ensure all required GitHub Secrets are still configured
3. Expect dependency conflicts if running alongside Iceberg development
