# Time Travel Data Validation

Automatically monitor data quality by comparing current table state with historical snapshots using Snowflake's Time Travel feature.

## What It Does

- **Compares current vs historical data** (default: 1 hour ago)
- **Tracks row count changes** - alerts if row counts change significantly
- **Monitors null counts** - detects unexpected increases in null values
- **Generic & reusable** - works across all tables via configuration
- **CI/CD integration** - runs automatically after deployments

## Quick Start

### 1. Configure Tables to Monitor

Edit [`config/data_validation.yml`](../config/data_validation.yml):

```yaml
lookback_minutes: 60  # How far back to compare
row_count_threshold: 0.20  # Alert if row count changes >20%
null_threshold: 0.10  # Alert if null count changes >10%

tables:
  - schema: analytics
    table: fact_player_game_stats
    columns:
      - player_id
      - game_id
```

### 2. Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=NHL_WH
export SNOWFLAKE_DATABASE=NHL_DB
export SNOWFLAKE_ROLE=TRANSFORMER
```

### 3. Run Validation

```bash
# Run with default settings
make validate-data

# Or run with custom lookback
LOOKBACK_MINUTES=120 make validate-data

# Or adjust thresholds
ROW_COUNT_THRESHOLD=0.30 NULL_THRESHOLD=0.15 make validate-data
```

## Example Output

```
================================================================================
TIME TRAVEL DATA QUALITY VALIDATION REPORT
Lookback: 60 minutes
================================================================================

📊 staging.stg_games
--------------------------------------------------------------------------------
  ✅ Row count: 1,234 (was 1,230) (+0.3% change)
  ✅ game_id: 0 nulls (was 0)
  ✅ home_team_id: 0 nulls (was 0)
  ❌ away_team_id: 45 nulls (was 0) - WARNING: +inf% change in nulls

📊 analytics.fact_player_game_stats
--------------------------------------------------------------------------------
  ✅ Row count: 45,678 (was 45,234) (+1.0% change)
  ✅ player_id: 0 nulls (was 0)
  ❌ Row count: 50,000 (was 40,000) - WARNING: +25.0% change exceeds threshold

================================================================================
SUMMARY: 5 passed, 2 failed
================================================================================
```

## CI/CD Integration

### GitHub Actions

The validation runs automatically:
- **After dbt deployments** - validates transformed data
- **Every 4 hours** - continuous monitoring
- **Manual trigger** - on-demand validation

See [`.github/workflows/data-validation.yml`](../.github/workflows/data-validation.yml)

### Local Development

Before committing major changes:

```bash
# 1. Run your dbt transformations
dbt run

# 2. Wait a bit (or use a previous snapshot)
# 3. Make changes and run dbt again
dbt run

# 4. Validate the changes
make validate-data
```

## Use Cases

### 1. Post-Deployment Validation
Run after dbt transformations to ensure data quality hasn't degraded:
```bash
dbt run --target prod
make validate-data
```

### 2. Detect Data Pipeline Issues
Alert when upstream data sources have problems:
```yaml
tables:
  - schema: raw
    table: games_raw
    columns: [game_id, home_team_id]  # Null IDs indicate API issues
```

### 3. Monitor Backfill Operations
Track progress and detect issues during historical data loads:
```bash
# Compare with state before backfill started
LOOKBACK_MINUTES=240 make validate-data
```

### 4. Pre-Production Testing
Validate staging environment before promoting to production:
```bash
# Point at staging database
SNOWFLAKE_DATABASE=NHL_DB_STAGING make validate-data
```

## Configuration Best Practices

### What to Check vs What NOT to Check

**✅ DO check for nulls in:**
- **Critical identifiers**: `game_id`, `player_id`, `team_id` - these should never be null
- **Foreign keys**: Fields that join tables together
- **Timestamp fields**: `game_date`, `created_at` - needed for partitioning/filtering
- **Data pipeline metadata**: Fields populated by your ETL process

**❌ DON'T check for nulls in:**
- **Stat columns**: `goals`, `assists`, `time_on_ice` - nulls are normal when players don't play
- **Optional fields**: `penalty_minutes`, `power_play_goals` - not all games have these
- **Calculated fields**: Derived metrics that may legitimately be null

**Why this matters for NHL data:**
- Teams don't all play on the same days
- Players get scratched, injured, or traded
- Not every player records every stat in every game
- Increasing nulls in stat columns is **expected**, not an error

### Example Configuration

```yaml
# ✅ GOOD - Checks critical IDs only
tables:
  - schema: staging
    table: stg_player_game_stats
    columns:
      - player_id  # Should never be null
      - game_id    # Should never be null
      - team_id    # Should never be null

# ❌ BAD - Checks stats that are naturally null
tables:
  - schema: staging
    table: stg_player_game_stats
    columns:
      - goals      # DON'T - nulls are normal when player doesn't score
      - assists    # DON'T - nulls are normal
      - toi        # DON'T - nulls expected when player doesn't play
```

## Configuration Options

### Adjusting Thresholds for NHL Data

The default thresholds might need tuning based on your use case:

**Row Count Threshold (default: 20%)**

NHL game schedules vary significantly:
- **Light day**: 2-3 games = ~400-600 player stats
- **Busy day**: 15+ games = ~1800+ player stats  
- **Variance**: 50-200% day-over-day is **normal**

**When to lower threshold (10-20%):**
- Hourly validations during same game day
- Stable season schedule period
- Validating transformations that shouldn't change row counts

**When to raise threshold (75-100%):**
- Validating across multi-day windows
- Season transitions (regular season → playoffs)
- After planned backfills

**Null Count Threshold (default: 10%)**

Critical IDs should **always** have 0 nulls:
- If `game_id`, `player_id`, or `team_id` go from 0 → ANY nulls, that's a problem
- Even 1 null in 10,000 rows (0.01%) indicates API/pipeline issues
- Keep this threshold LOW (10% is reasonable)

**Example scenarios:**

```bash
# Strict validation for incremental loads (same day)
ROW_COUNT_THRESHOLD=0.15 make validate-data

# Lenient validation during busy periods
ROW_COUNT_THRESHOLD=1.0 make validate-data  # Allow 100% change

# After known backfill - disable row count check temporarily
# (Just check for null IDs)
ROW_COUNT_THRESHOLD=999 make validate-data
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOOKBACK_MINUTES` | 60 | Minutes to look back for comparison |
| `ROW_COUNT_THRESHOLD` | 0.20 | Max acceptable row count change (20%) |
| `NULL_THRESHOLD` | 0.10 | Max acceptable null count change (10%) |
| `SNOWFLAKE_ACCOUNT` | - | Required: Snowflake account identifier |
| `SNOWFLAKE_USER` | - | Required: Snowflake username |
| `SNOWFLAKE_PASSWORD` | - | Required: Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | NHL_WH | Warehouse to use |
| `SNOWFLAKE_DATABASE` | NHL_DB | Database to query |
| `SNOWFLAKE_ROLE` | TRANSFORMER | Role to use |

### Config File

Add more tables to monitor in `config/data_validation.yml`:

```yaml
tables:
  - schema: staging
    table: stg_games
    columns:  # Columns to check for nulls
      - game_id
      - game_date
      
  - schema: analytics  
    table: dim_players
    # Omit columns to only check row count
```

## How It Works

Uses Snowflake's Time Travel to query historical table states:

```sql
-- Current state
SELECT COUNT(*) FROM analytics.fact_player_game_stats;

-- Historical state (1 hour ago)
SELECT COUNT(*) FROM analytics.fact_player_game_stats 
AT(OFFSET => -3600);

-- Compare them
```

No additional storage or setup required - Time Travel is always available.

## Limitations

- Requires Snowflake Time Travel (1-90 day retention depending on edition)
- Can't look back further than your retention period
- Queries consume warehouse compute credits
- Best for structural changes, not detailed content validation

## Integration with dbt

You can also create dbt tests that use Time Travel:

```sql
-- tests/time_travel_row_count.sql
WITH current_count AS (
    SELECT COUNT(*) as cnt FROM {{ ref('fact_player_game_stats') }}
),
historical_count AS (
    SELECT COUNT(*) as cnt 
    FROM {{ ref('fact_player_game_stats') }}
    AT(OFFSET => -3600)
)
SELECT 
    c.cnt as current,
    h.cnt as historical,
    ((c.cnt - h.cnt) / h.cnt * 100.0) as pct_change
FROM current_count c
CROSS JOIN historical_count h
WHERE ABS((c.cnt - h.cnt) / h.cnt) > 0.20  -- Fail if >20% change
```

## Troubleshooting

**Error: "Invalid data retention time"**
- Your Snowflake edition may have shorter retention (Standard = 1 day)
- Reduce `LOOKBACK_MINUTES` to stay within retention period

**Error: "SQL compilation error: Table does not exist"**
- Table was created recently (less than lookback period)
- Exclude it from validation or reduce lookback time

**All checks pass but data seems wrong**
- This tool validates *changes*, not absolute correctness
- Use dbt tests for business logic validation
- Use this tool to detect unexpected *variations*
