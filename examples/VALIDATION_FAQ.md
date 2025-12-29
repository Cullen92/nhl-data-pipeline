# Time Travel Validation - Quick Answers

## "Should this validation fail?"

### ❌ SHOULD FAIL (Real Problems)

**Null IDs appearing:**
```
❌ game_id: 23 nulls (was 0)
```
→ **Action**: Check NHL API, review ingestion logs, this is a bug

**Massive unexpected spike:**
```
❌ Row count: 100,000 (was 40,000) (+150% change)
```
→ **Action**: Check for duplicate loads, verify DAG didn't run twice

**IDs that were fine now broken:**
```
❌ player_id: 150 nulls (was 0)
```
→ **Action**: Data corruption, API issue, or join problem in dbt

---

## ✅ SHOULD PASS (Expected Variance)

**Normal game day variance:**
```
✅ Row count: 47,234 (was 45,234) (+4.4% change)
```
→ 2 more games played today, totally normal

**Steady state - no games:**
```
✅ Row count: 45,234 (was 45,234) (0% change)
```
→ Off-day or validation ran between loads

**All critical IDs present:**
```
✅ game_id: 0 nulls (was 0)
✅ player_id: 0 nulls (was 0)
✅ team_id: 0 nulls (was 0)
```
→ Perfect!

---

## MIGHT FAIL (Context Dependent)

**Large row count change:**
```
❌ Row count: 50,000 (was 40,000) (+25% change)
```

**Could be legitimate:**
- Planned backfill running
- Playoff season (more games)
- Season opener week (busy schedule)
- Comparing weekend (many games) vs weekday (few games)

**Could be a problem:**
- Duplicate load (check Airflow logs)
- Cartesian join in dbt model
- Same game data loaded multiple times

**How to tell:**
1. Check Airflow for duplicate runs
2. Query Snowflake: `SELECT game_date, COUNT(*) FROM fact_player_game_stats GROUP BY game_date ORDER BY 2 DESC`
3. Look for duplicate game_ids: `SELECT game_id, COUNT(*) FROM fact_player_game_stats GROUP BY 1 HAVING COUNT(*) > 400`

---

## Adjusting Thresholds

### Your data is failing but shouldn't?

**Busy game days (10+ games):**
```bash
# Allow larger variance
ROW_COUNT_THRESHOLD=0.75 make validate-data
```

**After backfill operations:**
```bash
# Temporarily disable row count check
ROW_COUNT_THRESHOLD=999 make validate-data
```

**Hourly validation (should be stable):**
```bash
# Stricter threshold
ROW_COUNT_THRESHOLD=0.10 make validate-data
```

### Your data is passing but shouldn't?

**Thresholds too lenient:**
Edit `config/data_validation.yml`:
```yaml
row_count_threshold: 0.30  # Lower = stricter
null_threshold: 0.02       # Lower = stricter
```

---

## NHL Schedule Context

Understanding normal variance:

| Day Type | Games | Player Stats | Row Count Change |
|----------|-------|--------------|------------------|
| Off day | 0 | ~0 | 0% |
| Light day | 1-3 | 200-600 | ±10-30% |
| Regular day | 5-10 | 1000-2000 | ±30-70% |
| Busy day | 12-16 | 2400-3200 | ±70-150% |
| Playoffs (daily) | 1-4 | 200-800 | ±10-40% |

**Default 50% threshold covers most regular season days**

---

## Quick Decision Tree

```
Is it a critical ID field (game_id, player_id, team_id)?
├─ YES → Nulls should ALWAYS be 0
│         Any nulls = FAILURE ❌
└─ NO → It's a stat field
          Nulls are expected ✅
          (Don't check it)

Is row count changing?
├─ 0-25% → Normal ✅
├─ 25-75% → Probably normal, check context 🤔
└─ >75% → Investigate (backfill? duplicate load?) ⚠️
```

---

## When in doubt...

**Run Time Travel query manually:**
```sql
-- See what changed
SELECT 
    (SELECT COUNT(*) FROM analytics.fact_player_game_stats) as current,
    (SELECT COUNT(*) FROM analytics.fact_player_game_stats 
     AT(OFFSET => -3600)) as one_hour_ago,
    current - one_hour_ago as diff;

-- Check for duplicate game_ids
SELECT game_id, game_date, COUNT(*) as player_count
FROM analytics.fact_player_game_stats
GROUP BY game_id, game_date
HAVING COUNT(*) > 400  -- Normal game has ~40 players * 2 teams = ~80-100 rows
ORDER BY player_count DESC;
```