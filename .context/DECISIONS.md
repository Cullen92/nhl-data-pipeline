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

<!-- Add new decisions above this line -->
