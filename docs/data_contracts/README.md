# Data Contracts

This folder documents the structure of external API responses that feed into our pipeline.

## Purpose

- **Reference** when building silver layer transformations
- **Onboarding** for new contributors to understand data sources
- **Debugging** when data doesn't match expectations
- **Change detection** to notice when APIs evolve

## Data Sources

| Source | Folder | Description |
|--------|--------|-------------|
| NHL API | [nhl_api/](nhl_api/) | Official NHL game data |
| Odds API | odds_api/ | Betting odds and props |

## Contract Format

Each contract is a YAML file with:

```yaml
name: Human-readable name
endpoint: API endpoint path
description: What this data represents

fields:
  field_name:
    type: int | string | float | bool | object | array
    description: What it means
    example: Sample value

nested_types:
  TypeName:
    description: Reusable type definition
    fields: ...

silver_layer_notes:
  - Hints for transformations
```

## Relationship to Pipeline Layers

```
External API → Raw JSON (S3) → Bronze (Iceberg) → Silver (Iceberg)
     ↑                              ↑                    ↑
  Documented here            Payload stored        Fields extracted
                              as string            using these docs
```
