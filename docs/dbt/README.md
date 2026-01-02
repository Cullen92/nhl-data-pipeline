# dbt Documentation

This folder contains resources related to dbt documentation and data lineage.

## Viewing dbt Docs

### Live Site (GitHub Pages)
Once enabled, dbt documentation is automatically deployed to GitHub Pages:
- **URL**: `https://cullen92.github.io/nhl-data-pipeline/`

### Local Development
To view docs locally:

```bash
cd dbt_nhl
dbt docs generate
dbt docs serve
```

This opens an interactive browser with:
- **Lineage Graph**: Visual DAG of all models and their dependencies
- **Model Documentation**: Descriptions, columns, and tests
- **Source Freshness**: Data quality indicators

## What's Included

The dbt docs site includes:

| Section | Description |
|---------|-------------|
| **Lineage Graph** | Interactive visualization of data flow from bronze → silver → gold |
| **Model Details** | SQL, descriptions, columns, and tests for each model |
| **Source Definitions** | Raw data sources and their schemas |
| **Exposure References** | Downstream consumers (dashboards, reports) |

## Updating Documentation

1. Add/update descriptions in `schema.yml` files
2. Commit and push to `main` branch
3. GitHub Actions automatically regenerates and deploys docs

## Manual Deployment

To manually trigger a docs rebuild:
1. Go to **Actions** → **Deploy dbt Docs to GitHub Pages**
2. Click **Run workflow**
