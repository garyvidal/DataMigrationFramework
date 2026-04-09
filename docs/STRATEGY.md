# DataMigrationFramework — Strategic Review

*Captured: 2026-04-08*

---

## Is this useful to anyone?

Yes — for a specific niche. It's a purpose-built ETL tool for migrating relational database data (PostgreSQL, Oracle, SQL Server, MySQL) into MarkLogic. That's a real pain point with no good visual tooling in the MarkLogic ecosystem.

**Who would use it:**
- MarkLogic shops migrating legacy RDBMS data
- Consultants/integrators doing RDBMS→MarkLogic migrations
- Teams who want a visual mapping tool rather than hand-writing corb or MLCP configs

**Current strengths:**
- Schema introspection → visual document mapping
- XML/JSON generation with JS-computed custom fields
- Spring Batch cursor pipeline (not naive pagination)
- Doc-level MarkLogic security (permissions/collections/quality)
- Portable migration packages for CI/CD or handoffs
- React frontend + REST API + CLI — three usable surfaces
- Prometheus/Grafana observability

---

## Should we extend to MongoDB/BSON?

**No.** MongoDB already has **MongoDB Relational Migrator** — a free, official, polished GUI tool that does exactly RDBMS→MongoDB mapping and migration. Competing with MongoDB Inc.'s own product is not a viable path. The MarkLogic niche is where this tool has a real gap to fill.

---

## How does this compare to MarkLogic Data Hub Framework (DHF)?

DHF is an opinionated, end-to-end data integration platform built on MarkLogic:
- Ingest → Harmonize → Curate pipeline model
- Staging/final/jobs database architecture
- SmartMastering for entity deduplication
- Entity Services integration
- gradle-based deployment

**Key insight: DHF and this tool are not competing — they're complementary.**

DHF is a platform for building a data hub. It assumes you already know what data you want and how to get it. The RDBMS ingestion part is left to you — you write custom ingest steps.

**This tool is the missing front door for relational data.** It handles what DHF doesn't: visually mapping a relational schema to documents, generating XML/JSON, and driving the batch load — without requiring a DHF project structure or deep MarkLogic expertise upfront.

| Capability | This Framework | DHF |
|---|---|---|
| RDBMS schema introspection | Yes — visual, live | No |
| Visual document mapping | Yes | Basic |
| RDBMS batch migration | Yes — Spring Batch | Via custom steps only |
| Custom field JS functions | Yes (Rhino) | Yes (server-side SJS) |
| XML + JSON output | Yes | Yes |
| Doc-level security | Yes | Yes |
| Entity Services alignment | No | Yes — first-class |
| Harmonization / mastering | No | Yes |
| Multi-source orchestration | No | Yes |
| Learning curve | Low | High |
| Setup time | Minutes | Days |

---

## Strategic options

1. **Standalone tool** — target teams that don't use DHF and need to get SQL data into MarkLogic fast. Low barrier, simple story.

2. **DHF companion** — generate DHF-compatible ingest step artifacts from mappings. This tool becomes the visual mapping layer that feeds a DHF project. Real gap DHF has.

3. **Entity Services alignment** — if mapping output conforms to an ES model descriptor, the tool slots directly into DHF/DHS. Highest-value integration point for existing MarkLogic enterprise customers.

Option 2 or 3 would make this significantly more attractive to MarkLogic's existing enterprise customer base.

---

## Priority roadmap

### Phase 1 — Make it production-safe
- **Pause/resume/retry** on batch job failure (Spring Batch checkpointing)
- **Validation dry-run** — pre-flight report of failures before writing any documents
  - `MigrationValidationService` exists but is incomplete

### Phase 2 — Make it sticky
- **Schema diff / drift detection** — detect when source DB schema changes and flag affected mappings
- **Reusable mapping templates** — saveable patterns for common transformations

### Phase 3 — Make it distributable
- **Containerized deployment** — Dockerfile + full compose (app + observability)
- **Install-in-5-minutes docs**

### Phase 4 — Deepen MarkLogic differentiation
- **Entity Services alignment** — generate mappings conforming to ES model descriptors
- **DHF ingest step export** — output DHF-compatible step artifacts
- **Temporal document support** — system-time / valid-time axis on insert
- **Smart URI generation** — configurable URI patterns per entity
- **MarkLogic alerting integration** — trigger alerts post-ingest

---

## Key competitive advantages over MLCP / corb2

- Visual schema introspection — no hand-writing configs
- Live document preview before migration
- Custom JS field functions (Rhino)
- Doc-level security configuration in the UI
- Portable migration packages
- CLI + REST API + UI — three surfaces
