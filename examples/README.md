# Examples Directory

Quick reference for the NHL Data Pipeline.

## Time Travel Validation

### Quick Reference

**Decision guide:**
- [VALIDATION_FAQ.md](VALIDATION_FAQ.md) - "Should this fail?" decision tree and troubleshooting

### Running Validation

```bash
# Basic validation (uses config/data_validation.yml)
make validate-data

# Custom lookback period (compare with 2 hours ago)
LOOKBACK_MINUTES=120 make validate-data

# Adjust thresholds for busy game days
ROW_COUNT_THRESHOLD=0.75 make validate-data

# After backfill (allow large changes)
ROW_COUNT_THRESHOLD=999 make validate-data

# Strict validation for incremental loads
ROW_COUNT_THRESHOLD=0.10 make validate-data
```

### Setup

```bash
# Set Snowflake credentials
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user  
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=TRANSFORMING
export SNOWFLAKE_DATABASE=NHL_DB
export SNOWFLAKE_ROLE=TRANSFORMER
```

### Configuration

Edit [config/data_validation.yml](../config/data_validation.yml) to:
- Add/remove tables to monitor
- Adjust default thresholds
- Change lookback period

See [docs/time_travel_validation.md](../docs/time_travel_validation.md) for full documentation.
