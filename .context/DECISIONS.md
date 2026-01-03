# Decision Log

> A chronological record of significant decisions made during development. Each entry provides context for future collaborators (human or AI).

---

## Template

```markdown
## YYYY-MM-DD: [Short Decision Title]

**Status:** Accepted | Superseded | Deprecated

**Context:** What situation prompted this decision?

**Decision:** What was decided?

**Alternatives Considered:**
- Option A: Why rejected
- Option B: Why rejected

**Consequences:**
- Positive: Benefits gained
- Negative: Tradeoffs accepted
```

---

## 2024-12-29: Established Context Tracking System

**Status:** Accepted

**Context:** Working with LLM agents across multiple machines and sessions. Context was being lost between sessions, requiring repeated explanation of architecture and past decisions.

**Decision:** Create a `.context/` directory with:
- `ARCHITECTURE.md` — Stable project design documentation
- `DECISIONS.md` — Running log of significant choices
- `CONVENTIONS.md` — Coding standards and patterns

**Alternatives Considered:**
- Single mega-doc: Would become unwieldy
- Wiki: Adds external dependency, harder to version
- README only: Already exists but should stay lean for onboarding

**Consequences:**
- Positive: LLMs can quickly understand project context
- Positive: Decisions are preserved with rationale
- Positive: Shareable across machines via git
- Negative: Requires discipline to maintain

---

## 2024-12-28: Snowflake-Airflow Connection via UI

**Status:** Accepted (temporary)

**Context:** Needed to connect Airflow (MWAA) to Snowflake for data warehouse operations during development.

**Decision:** Configure the Snowflake connection using Airflow's UI-based Connections interface with hardcoded credentials.

**Alternatives Considered:**
- IAM roles: Preferred for production, but adds complexity for initial dev work
- Environment variables: Still requires secrets management
- AWS Secrets Manager: Planned for future, overkill for dev

**Consequences:**
- Positive: Quick setup, unblocks development work
- Negative: Credentials hardcoded in UI — not production-ready
- Negative: Not portable across environments

**Future Plan:** Migrate to IAM role-based authentication for production deployment, following AWS/Snowflake best practices for credential management.

---

## 2024-12-29: Snowflake Credentials in GitHub Actions Secrets

**Status:** Accepted (temporary)

**Context:** Needed to run the Time Travel data validation workflow in GitHub Actions, which requires Snowflake connectivity.

**Decision:** Store `SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD` as GitHub Actions repository secrets.

**Alternatives Considered:**
- OIDC/Workload Identity Federation: Preferred approach, avoids storing credentials
- AWS Secrets Manager with OIDC: More secure but requires additional setup
- Skip CI validation: Would lose automated data quality checks

**Consequences:**
- Positive: Quick unblock for CI/CD pipeline testing
- Negative: Password-based auth stored in GitHub — not ideal for production
- Negative: Credentials must be rotated manually

**Future Plan:** Migrate to OIDC-based authentication (GitHub → AWS → Snowflake) or Snowflake key-pair authentication to eliminate password storage.

---

## 2025-12-30: Denormalized Data Warehouse Architecture

**Status:** Accepted

**Context:** Designing the data warehouse layer for NHL analytics with goals of:
- Supporting ML predictions (SOG player props)
- Learning modern ELT/analytics engineering patterns
- Preparing for data analyst roles at modern tech companies
- Eventually supporting shot-level heat map analysis

**Decision:** Implement a **modern denormalized/hybrid approach** using medallion architecture (bronze/silver/gold) rather than strict Kimball star schema.

**Architecture:**
- **Bronze Layer:** Raw, immutable views over source data (no transformations)
- **Silver Layer:** Cleaned, deduplicated facts and sparse dimensions
  - Dimensions: Sparse, descriptive only (dim_player, dim_team, dim_date)
  - Facts: Some denormalization (e.g., player_name in fact for query convenience)
  - Multiple grains: player-per-game initially, shot-level events later
- **Gold Layer:** Pre-aggregated, wide tables optimized for specific use cases
  - Rolling averages, season summaries, matchup analysis
  - ML-ready feature tables

**Alternatives Considered:**
- Pure Kimball Star Schema: Rejected due to:
  - More JOINs required for self-service analytics
  - Less common in modern cloud data warehouses
  - Optimizes for storage over compute (outdated with cloud economics)
- Fully Denormalized OBT (One Big Table): Rejected due to:
  - Loss of dimensional model clarity
  - Harder to maintain and reason about
  - Doesn't support multiple grains cleanly

**Consequences:**
- Positive: Modern approach used at data-mature tech companies (dbt + Snowflake)
- Positive: Better fit for ML use cases (wide feature tables)
- Positive: Easier self-service analytics (less complex JOINs)
- Positive: Cloud warehouse optimization (columnar storage handles wide tables)
- Positive: Supports multiple fact tables at different grains (game-level, shot-level)
- Negative: Trades dimensional modeling "purity" for pragmatism
- Negative: Some data duplication (denormalized attributes in facts)

**Implementation Plan:**
1. Start with player-per-game grain for SOG predictions
2. Add shot-level events later for heat maps
3. Keep dimensions orthogonal to grain for easy extension

---

## 2025-12-30: Jinja Templating for Repetitive SQL Patterns

**Status:** Accepted

**Context:** Building dim_player required extracting player data from 4 identical structures (home/away × forwards/defense), resulting in repetitive SQL with 4 nearly-identical SELECT statements joined by UNION.

**Decision:** Use dbt's Jinja templating to loop over team/position combinations and generate SQL dynamically.

**Implementation:**
```sql
{%- set combinations = [
    {'team': 'homeTeam', 'type': 'F', 'path': 'forwards'},
    {'team': 'homeTeam', 'type': 'D', 'path': 'defense'},
    {'team': 'awayTeam', 'type': 'F', 'path': 'forwards'},
    {'team': 'awayTeam', 'type': 'D', 'path': 'defense'}
] %}
{%- for combo in combinations %}
SELECT ... FROM ... {{ combo.team }}:{{ combo.path }}
{%- if not loop.last %} UNION {%- endif %}
{%- endfor %}
```

**Alternatives Considered:**
- Copy-paste SQL: Simple but unmaintainable (68 lines → 46 lines with Jinja)
- Macro functions: More reusable but overkill for single model
- UNION ALL with CASE statements: Verbose, less clear intent

**Consequences:**
- Positive: DRY principle - change logic once, applies to all combinations
- Positive: Easy to extend (add goalies = 1 line in array)
- Positive: Clear intent - "loop over team/position combinations"
- Positive: Reduced file from 68 lines (hypothetical copy-paste version) to 46 lines (32% reduction)
- Negative: Learning curve for Jinja syntax
- Negative: Whitespace control gotchas (`{%-` vs `{%`)
- Negative: Harder to debug (must check compiled SQL in target/)

**Key Lesson:** Jinja whitespace control - use `{%-` carefully as it strips newlines and can cause syntax errors (e.g., "seasonWITH" bug).

---

## 2025-12-30: Sparse Dimensions Over Pre-Populated Dimensions

**Status:** Accepted

**Context:** Building dim_team and dim_player required deciding whether to pre-load all 32 NHL teams or let dimensions grow organically from actual game data.

**Decision:** Use **sparse dimensions** that populate from actual data appearing in games, rather than pre-loading complete reference data.

**Implementation:**
- dim_team: DISTINCT team data from boxscore snapshots (currently 10 teams)
- dim_player: DISTINCT player data from game appearances (currently 829 players)
- Both grow automatically as new data arrives

**Alternatives Considered:**
- Pre-populated dimensions: Seed CSV with all 32 teams, historical player list
  - Pros: Complete reference data, stable IDs
  - Cons: Requires external data source, maintenance burden, orphaned records
- Type 2 SCD: Track changes over time (player team changes, name changes)
  - Pros: Historical accuracy
  - Cons: Added complexity, not needed for current use case

**Consequences:**
- Positive: Zero-maintenance - new teams/players appear automatically
- Positive: Only real data - no orphaned dimension records
- Positive: Simpler pipeline - no seed file management
- Positive: Easy to backfill complete reference data when needed from NHL API

**Note:** Currently showing 10 of 32 teams based on games in bronze layer. Full team roster (with conference/division) can be backfilled via one-time script pulling from NHL teams API endpoint.

---

## 2025-12-30: Denormalized Attributes in Fact Tables

**Status:** Accepted

**Context:** Building fact_player_game_stats with foreign keys to dim_player and dim_team. Needed to decide whether to store only IDs (pure Kimball) or denormalize commonly-queried attributes.

**Decision:** Include denormalized attributes (player_name, team_abbrev) in fact table alongside foreign keys for query performance.

**Implementation:**
```sql
SELECT
    ps.game_id,
    ps.player_id,           -- FK to dim_player
    ps.team_id,             -- FK to dim_team
    p.player_name,          -- Denormalized from dim_player
    t.team_abbrev,          -- Denormalized from dim_team
    ps.goals,
    ps.assists,
    ...
FROM player_stats ps
LEFT JOIN dim_player p ON ps.player_id = p.player_id
LEFT JOIN dim_team t ON ps.team_id = t.team_id
```

**Alternatives Considered:**
- Pure Kimball (IDs only): Requires JOIN for every query needing names
- Fully denormalized: Include ALL dimension attributes (overkill)
- Separate OBT for common queries: Adds maintenance burden

**Consequences:**
- Positive: **Faster queries** - most analytics need player/team names
- Positive: Self-service friendly - analysts don't need dimension knowledge
- Positive: Columnar storage minimizes space overhead
- Negative: Data duplication (~20KB per 22K rows for names)
- Negative: Name changes require fact table updates (rare for NHL players)
- Negative: Deviates from strict Kimball methodology

**Rationale:** Modern cloud warehouses (Snowflake) make this trade-off favorable:
- Columnar storage compresses strings efficiently
- Query performance > storage cost in cloud economics
- Compute scales independently of storage

---

## 2025-12-30: Filtering Completed Games in Silver Layer

**Status:** Accepted

**Context:** Bronze layer contains snapshots of games in all states (FUT = future scheduled, LIVE = in progress, OFF = completed). Need to decide where to filter for completed games.

**Decision:** Filter to `game_state = 'OFF'` (completed games only) in **silver layer fact tables**, not bronze views.

**Implementation:**
- Bronze: Pass through all game_state values unchanged
- Silver fact tables: `WHERE game_state = 'OFF'`
- Excludes in-progress and future games from analytics

**Alternatives Considered:**
- Filter in bronze: Would lose ability to analyze live games later
- Filter in gold: Keeps incomplete games in silver layer
- Separate fact tables: fact_live_games, fact_completed_games (unnecessary complexity)

**Consequences:**
- Positive: Bronze maintains immutability (all data preserved)
- Positive: Silver facts represent only analyzable completed games
- Positive: Can add live game analysis later without touching bronze
- Negative: Can't analyze in-progress games without separate model
- Negative: Game count differs between bronze (all) and silver (completed only)

**Future Extension:** Could create `fact_live_game_updates` if real-time analysis needed.

---

## 2025-12-30: Most Recent Snapshot Pattern with QUALIFY

**Status:** Accepted

**Context:** Bronze tables may contain multiple snapshots per game (due to data refreshes, corrections, or incremental loads). Need to handle duplicates when building silver layer facts.

**Decision:** Use Snowflake's `QUALIFY` with `ROW_NUMBER()` window function to select most recent snapshot per game.

**Implementation:**
```sql
FROM bronze_game_boxscore_snapshots
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payload:id 
    ORDER BY partition_date DESC
) = 1
```

**Alternatives Considered:**
- CTE with WHERE clause: More verbose, less efficient
- GROUP BY with MAX(partition_date): Requires join back to get full row
- DISTINCT ON (PostgreSQL style): Not available in Snowflake
- LAST_VALUE() window: More complex logic

**Consequences:**
- Positive: Idempotent - reruns produce same results
- Positive: Handles late-arriving data automatically
- Positive: Efficient single-pass query (no self-join)
- Positive: Snowflake-native optimization
- Negative: Snowflake-specific syntax (not portable to other warehouses)
- Negative: Assumes partition_date reflects data freshness

**Note:** This pattern appears in both fact_game_results and fact_player_game_stats for consistency.

---

## 2025-12-30: Two-Level Position Hierarchy

**Status:** Accepted

**Context:** NHL player positions have specific codes (C, LW, RW, D) but analytics often need broader groupings (forwards vs defense). Needed to decide on data model.

**Decision:** Store both **position_code** (specific) and **position_type** (category) in dim_player and fact_player_game_stats.

**Implementation:**
- position_code: C, L, R, D, LW, RW (from API)
- position_type: F (forward) or D (defense) (derived)

**Alternatives Considered:**
- Single field: position_code only, require CASE in every query
- Computed column: Calculate position_type in SELECT, no storage
- Separate position dimension: Overkill for 2 categories

**Consequences:**
- Positive: Flexible analytics - can slice by specific or broad position
- Positive: Self-documenting - no need to remember C/L/R mapping
- Positive: Fast queries - no CASE statement overhead
- Negative: Slight data duplication (1 char per row)
- Negative: Inconsistency risk if derivation logic differs

**Use Cases Enabled:**
- "Goals by position type" (F vs D) - simple GROUP BY
- "Faceoff stats by center position" (C only) - specific filter
- ML features can use either level of granularity

---

## 2025-12-30: Comprehensive Relationship Testing Strategy

**Status:** Accepted

**Context:** Building dimensional model with foreign keys between fact and dimension tables. dbt supports relationship tests to validate referential integrity.

**Decision:** Implement relationship tests for **all foreign keys** in fact tables to validate that FKs exist in referenced dimensions.

**Implementation:**
```yaml
- name: game_id
  tests:
    - relationships:
        to: ref('fact_game_results')
        field: game_id
- name: player_id
  tests:
    - relationships:
        to: ref('dim_player')
        field: player_id
```

**Test Coverage:**
- fact_game_results: date_key → dim_date, home_team_id → dim_team, away_team_id → dim_team (3 FK tests)
- fact_player_game_stats: game_id → fact_game_results, player_id → dim_player, date_key → dim_date, team_id → dim_team (4 FK tests)

**Alternatives Considered:**
- No testing: Fast but risky (orphaned records go undetected)
- Sample testing: Test subset of FKs (incomplete coverage)
- Database constraints: Snowflake doesn't enforce FKs (metadata only)
- Manual validation queries: Not automated, easy to forget

**Consequences:**
- Positive: **Catches data quality issues** - orphaned records found immediately
- Positive: Self-documenting FK relationships in code
- Positive: CI/CD integration - PRs fail on broken relationships
- Positive: Confidence in JOIN correctness for analytics
- Negative: Slower dbt test runs (7 relationship tests = 7 full scans)
- Negative: Tests fail if dimensions incomplete (sparse dimension risk)

**Result:** 54 total passing tests across silver layer models, including 7 relationship tests validating referential integrity.

---

## 2026-01-02: Warehouse-Native A/B Testing with Eppo (Planned)

**Status:** Planned

**Context:** Evaluating tools for A/B testing experience (Amplitude Experiment, Optimizely, StatSig, LaunchDarkly, Eppo). Need to demonstrate experimentation skills in a data engineering context, not a traditional product context.

**Decision:** Plan integration with **Eppo** for warehouse-native experimentation to compare defensive analysis models (shots against).

**Experiment Concept:**
- **Control:** Position-based model (F vs D shot weighting from `team_shots_against_by_position`)
- **Treatment:** Location-based model (zone weighting from `fact_shot_events` x/y coordinates)
- **Metric:** Which model better predicts goals against?

**Alternatives Considered:**
- Amplitude/Optimizely: More product-focused, less natural fit for data pipelines
- LaunchDarkly: Good for feature flags but not true experimentation
- StatSig: Has warehouse integrations but Eppo is more SQL-native
- xG experimentation: Considered but SOG analysis aligns better with existing work

**Why Eppo:**
- Warehouse-native (connects directly to Snowflake)
- SQL-based metric definitions (fits dbt workflow)
- Designed for analytics engineering teams
- Free tier available for evaluation

**Consequences:**
- Positive: Genuine experimentation use case tied to existing shot analysis
- Positive: Demonstrates modern A/B testing tooling in data engineering context
- Positive: No changes to existing models required (additive)
- Negative: Requires Eppo account setup and Snowflake connection
- Negative: Needs sufficient game data for statistical significance

**Implementation Notes:**
See `dbt_nhl/TEAM_SHOTS_README.md` for detailed experiment design and prerequisites.

---

## 2026-01-02: GitHub Pages for dbt Documentation & Lineage

**Status:** Accepted

**Context:** Need to showcase data lineage publicly in GitHub repo. dbt generates interactive documentation with lineage graphs, but `target/` is correctly gitignored (contains ephemeral build artifacts).

**Decision:** Deploy dbt docs to GitHub Pages via automated GitHub Actions workflow.

**Implementation:**
- New workflow: `.github/workflows/dbt-docs.yml`
- Triggers on push to `main` when `dbt_nhl/**` changes
- Generates docs and deploys to GitHub Pages
- Manual trigger available via `workflow_dispatch`

**What's Showcased:**
- Interactive lineage graph (model → model dependencies)
- Column-level documentation from `schema.yml`
- Test coverage and data quality indicators
- SQL code for all models

**Alternatives Considered:**
- Commit `target/` folder: Bloats repo, stale artifacts
- Manual screenshots: Not interactive, goes stale
- External hosting (Netlify): Adds dependency, GitHub Pages is free
- dbt Cloud: Would provide column-level lineage but costs $$$

**Consequences:**
- Positive: Public, interactive lineage accessible via URL
- Positive: Auto-updates on merge to main
- Positive: Demonstrates CI/CD and documentation practices
- Positive: Zero cost (GitHub Pages is free)
- Negative: Requires GitHub Pages to be enabled in repo settings
- Negative: No column-level lineage (dbt Cloud feature)

**Setup Required:**
1. Go to repo **Settings** → **Pages**
2. Set Source to "GitHub Actions"
3. Push changes to trigger first deployment

---

## 2026-01-03: Tableau Public Limitations and Bruins-Only Focus

**Status:** Accepted

**Context:** Attempted to use Tableau Public with Google Sheets as the data source for automatic daily refresh. Encountered significant limitations:
- Tables with 48K+ rows cause "insufficient permissions" errors via Google Sheets
- CSV files embed data in workbook, requiring manual republish to update
- No REST API for programmatic refresh (unlike Tableau Server/Cloud)

**Decision:** 
1. **Short-term:** Focus exclusively on Boston Bruins data to stay within Google Sheets row limits (~25K rows max)
2. **Long-term:** Evaluate alternative visualization tools that support direct Snowflake connections and scheduled refresh

**Bruins-specific models created:**
- `bruins_player_shot_locations` — Player shot heatmap data
- `bruins_team_shot_locations` — Team-level shot heatmap data
- `bruins_shot_events` — Individual shot events
- `bruins_next_opponent` — Auto-updating next game opponent info
- `bruins_opponent_shot_locations` — Opponent tendencies for scouting

**Alternatives Considered:**
- Full league data in Tableau: Rejected due to Google Sheets row limits
- CSV-only approach: Rejected due to no automatic refresh capability
- Immediate migration to Streamlit: Deferred to complete Phase 1 first

**Future Tool Candidates:**
| Tool | Cost | Auto-Refresh | Custom Viz | Notes |
|------|------|--------------|------------|-------|
| Streamlit | Free | ✅ Direct Snowflake | ✅ Full Python | Growing in DE community |
| Looker Studio | Free | ✅ Via connectors | ⚠️ Limited | Google ecosystem |
| Apache Superset | Free (self-host) | ✅ Scheduled | ⚠️ Limited | Used by Airbnb, Dropbox |
| Metabase | Free (self-host) | ✅ Scheduled | ⚠️ Limited | Easy setup |

**Consequences:**
- Positive: Bruins-focused dashboard works with automatic refresh
- Positive: Smaller data volumes = faster queries and lower Snowflake costs
- Positive: Clear path to evaluate better tools in Phase 2
- Negative: Dashboard limited to single team analysis
- Negative: Full league analysis requires manual CSV workflow

---

<!-- Add new decisions above this line -->
