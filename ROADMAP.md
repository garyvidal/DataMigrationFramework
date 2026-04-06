# DataMigrationFramework Feature Roadmap

## Possible Feature Roadmap

Based on the project’s current backend and frontend scope, here are strong roadmap candidates:

### 1. Core migration capability improvements
- MarkLogic document security support
  - document permissions, collections, quality, metadata
  - job-level overrides and project inheritance
- JSON mapping expansion
  - richer `JsonTableMapping` / `JsonColumnMapping`
  - nested objects, arrays, inline objects, custom transform functions
- XML/JSON generation preview enhancements
  - live sample row preview
  - schema-driven document validation
  - side-by-side XML/JSON compare mode

### 2. Migration execution and automation
- robust batch job orchestration
  - pause / resume / retry / cancel
  - checkpointing and restart support
- scheduling and recurring migrations
  - cron-style jobs
  - automatic re-run on schema changes
- migration package improvements
  - portable import/export with versioning
  - CLI-driven package execution with overrides
  - package validation and dry-run mode

### 3. Observability and operations
- migration dashboard
  - progress, throughput, error summaries
  - per-entity and per-step metrics
- logging and error triage
  - document-level failure details
  - retry recommendations
- monitoring hooks
  - Prometheus / Grafana metrics
  - audit trail for project changes and migration runs

### 4. UX / product polish
- smarter schema diagram UX
  - expand/collapse relationship groups
  - drag-and-drop join creation
  - search/filter schema objects
- reusable mapping patterns
  - templates for common document shapes
  - clone/reuse entity mappings across projects
- security/configuration UI
  - role/capability editor
  - collection/tag input
  - security preview and validation
- better project management
  - multi-project tabs
  - saved connection folders
  - import/export project snapshots

### 5. Advanced data capabilities
- schema diff and migration impact analysis
  - detect changed tables/columns
  - suggest mapping updates
- validation rules and data quality checks
  - type mismatch warnings
  - required field enforcement
- support for additional sources
  - more JDBC dialects
  - file-based sources (CSV/JSON)
  - eventually non-relational sources if needed

### 6. Deployment / platform readiness
- containerized deployment
  - Dockerfile / compose support
  - Kubernetes or Azure container app manifest
- configuration / secrets management
  - external config file support
  - encrypted connection vault
- multi-environment support
  - dev/staging/prod profiles
  - selectable MarkLogic targets

---

## Recommended roadmap structure

1. **Phase 1**: complete core data model + security + mapping UX
2. **Phase 2**: add execution robustness, package automation, and monitoring
3. **Phase 3**: polish UX, add reusable templates, schema diff, and validation
4. **Phase 4**: platform readiness with containerization, cloud deployment, and advanced source support

This aligns with the existing backend/frontend split and keeps early work focused on the highest-value migration scenario: reliable MarkLogic ingestion with stronger security and operational visibility.
