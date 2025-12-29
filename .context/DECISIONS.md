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

<!-- Add new decisions above this line -->
