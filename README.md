# DataMigrationFramework — Backend API

A Spring Boot REST API that powers the RDBMS-to-MarkLogic data migration framework. It introspects relational database schemas, builds XML and JSON document mappings, previews generated documents using live data, executes large-scale batch migrations to MarkLogic, and supports portable migration packages for sharing and automation.

## Purpose

This backend enables:
- **Schema analysis** — introspect relational databases (PostgreSQL, Oracle, SQL Server, MySQL) using SchemaCrawler
- **Document generation** — build XML and JSON documents from mapping definitions and live RDBMS data
- **Custom field functions** — evaluate per-field JavaScript expressions (via Mozilla Rhino) for computed values in XML and JSON mappings
- **XSD schema generation** — derive XML Schema definitions from project mapping configurations
- **Batch migration** — run Spring Batch jobs backed by a cursor-driven pipeline with configurable worker threads
- **CLI migration** — launch and monitor migrations directly from the command line
- **Ingest transforms** — apply a server-side MarkLogic REST transform to every document on write (configurable per job via the UI or CLI)
- **Migration packages** — export a project + connections into a single portable JSON file; import via the UI or CLI
- **Project persistence** — store and retrieve migration projects, mappings, and connection credentials on the filesystem
- **Observability** — Prometheus metrics endpoint + bundled Grafana dashboard via Docker Compose
- **Frontend support** — serves the bundled React frontend in production

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 23 |
| Framework | Spring Boot 3.4.2 |
| Build | Maven |
| Schema introspection | SchemaCrawler 16.25 |
| Batch processing | Spring Batch (H2 metadata DB) |
| MarkLogic client | MarkLogic Client API 8.0 |
| JDBC drivers | PostgreSQL 42.7, Oracle ojdbc11 23.7, MSSQL 12.8 |
| JavaScript engine | Mozilla Rhino 1.7.15 (custom field functions) |
| CLI parsing | Picocli 4.7 |
| Credentials | AES-256-GCM encryption at rest |
| Metrics | Spring Actuator + Micrometer (Prometheus) |
| Observability | Prometheus + Grafana (Docker Compose) |
| Frontend bundling | `frontend-maven-plugin` (Node 22 + Vite) |

## Prerequisites

- Java 23
- Maven 3.8+
- A running supported source database (PostgreSQL, Oracle, SQL Server, or MySQL)
- A running MarkLogic server as the migration target

## Getting Started

```bash
# Build (Java + React frontend)
mvn clean package

# Run the API server
java -jar target/DataMigrationFramework-1.0.0.jar
```

The API starts at **`http://localhost:9390`**.

The React frontend is bundled into the JAR and served at the root path. During development, the frontend runs separately at `http://localhost:5173`.

---

## Configuration

All built-in defaults live inside the JAR in `application.properties`. You can override any property without recompiling using one of the methods below, listed from highest to lowest priority.

### 1. Command-line arguments

Append any property as `--key=value` when starting the JAR:

```bash
java -jar DataMigrationFramework-1.0.0.jar \
  --server.port=8080 \
  --cors.allowed-origins=https://myapp.example.com \
  --migration.marklogic.batcher.thread-count=8
```

### 2. Environment variables

Spring Boot maps environment variables to properties automatically by uppercasing and replacing `.` and `-` with `_`:

| Property | Environment Variable |
|---|---|
| `server.port` | `SERVER_PORT` |
| `cors.allowed-origins` | `CORS_ALLOWED_ORIGINS` |
| `migration.marklogic.batcher.batch-size` | `MIGRATION_MARKLOGIC_BATCHER_BATCH_SIZE` |
| `migration.marklogic.batcher.thread-count` | `MIGRATION_MARKLOGIC_BATCHER_THREAD_COUNT` |
| `logging.level.root` | `LOGGING_LEVEL_ROOT` |

```bash
# Linux / macOS
export SERVER_PORT=8080
export CORS_ALLOWED_ORIGINS=https://myapp.example.com
java -jar DataMigrationFramework-1.0.0.jar

# Windows (PowerShell)
$env:SERVER_PORT = "8080"
java -jar DataMigrationFramework-1.0.0.jar
```

### 3. External properties file (recommended for persistent config)

Place an `application.properties` file next to the JAR in a `config/` subdirectory. Spring Boot picks it up automatically at startup — no flags required:

```
deployment/
├── DataMigrationFramework-1.0.0.jar
└── config/
    └── application.properties     <-- your overrides go here
```

A ready-to-edit template with all configurable properties and their defaults is included at [`config/application.properties`](config/application.properties) in this repository. Copy it to your deployment directory and edit as needed.

Alternatively, place the file directly alongside the JAR (not in a subdirectory):

```
deployment/
├── DataMigrationFramework-1.0.0.jar
└── application.properties
```

### 4. Custom config location

Point to any file or directory with `--spring.config.additional-location`:

```bash
java -jar DataMigrationFramework-1.0.0.jar \
  --spring.config.additional-location=/etc/dmf/application.properties
```

This is additive — the built-in defaults still apply for anything not overridden.

### Configurable Properties Reference

| Property | Default | Description |
|---|---|---|
| `server.port` | `9390` | API server port |
| `cors.allowed-origins` | `http://localhost:5176,http://localhost:5173` | Comma-separated allowed browser origins |
| `migration.marklogic.batcher.batch-size` | `500` | Documents per MarkLogic HTTP request |
| `migration.marklogic.batcher.thread-count` | `8` | Parallel WriteBatcher writer threads |
| `migration.pipeline.worker-threads` | `0` (auto) | Cursor pipeline worker threads; `0` = number of CPUs, max 16 |
| `migration.pipeline.queue-capacity` | `8` | Root-row batch queue depth between cursor and workers |
| `schemacrawler.schema.pattern.exclude` | `dbo\|sys\|SYS\|...` | Regex of schema names to exclude from introspection |
| `logging.level.root` | *(Spring default)* | Root log level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `logging.level.com.nativelogix...` | `DEBUG` | Framework class log level |
| `logging.level.com.marklogic.client.datamovement` | `DEBUG` | MarkLogic WriteBatcher log level |
| `spring.main.banner-mode` | `console` | Set to `off` to suppress startup banner |

---

## Command Line Interface (CLI)

Migrations can be launched directly from the terminal without the web UI. The CLI activates via the `cli` Spring profile, which disables the embedded web server so the process exits cleanly when the migration finishes.

### Basic Usage

```bash
java -jar DataMigrationFramework-1.0.0.jar \
  --spring.profiles.active=cli \
  --project "My Project" \
  --marklogic-connection "ML Prod"
```

### All Options

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--project` | `-p` | Yes* | Project name or ID |
| `--marklogic-connection` | `-m` | Yes* | MarkLogic connection name or ID |
| `--package` | | No | Path to a migration package file. When provided, `--project` and `--marklogic-connection` are sourced from the file and are no longer required |
| `--source-connection` | `-s` | No | Source DB connection name or ID (defaults to the connection stored on the project) |
| `--source-password` | | No | Password for the source DB connection when using `--package`. Accepts plaintext or a pre-encrypted `ENC:...` value |
| `--marklogic-password` | | No | Password for the MarkLogic connection when using `--package`. Accepts plaintext or a pre-encrypted `ENC:...` value |
| `--directory` | `-d` | No | MarkLogic document directory path (default: `/`). Supports `{rootElement}` and `{index}` placeholders |
| `--collection` | `-c` | No | MarkLogic collection(s) to tag written documents. Comma-separated or repeatable |
| `--poll-interval` | | No | Progress poll interval in milliseconds (default: `1000`) |
| `--dry-run` | | No | Count source records and validate config but do not write any documents to MarkLogic |
| `--validate-only` | | No | Run pre-flight validation checks and print the report. Exit `0` if all checks pass, `1` if any FAIL |
| `--transform` | | No | Name of a server-side MarkLogic REST transform to apply on ingest |
| `--transform-param` | | No | Named parameter for the ingest transform, in `key=value` form. Repeatable (or comma-separated) |
| `--list-projects` | | No | Print all available projects and exit |
| `--list-connections` | | No | Print all source DB connections and exit |
| `--list-ml-connections` | | No | Print all MarkLogic connections and exit |
| `--help` | `-h` | No | Print usage and exit |

\* Required unless `--package` is provided.

### Examples

```bash
# Full example with collections and custom directory
java -jar DataMigrationFramework-1.0.0.jar \
  --spring.profiles.active=cli \
  --project "Orders Migration" \
  --marklogic-connection "ML Production" \
  --source-connection "Oracle XE" \
  --directory "/orders/{rootElement}/" \
  --collection orders,archive

# Run from a migration package file (registers project + connections if not already present)
java -jar DataMigrationFramework-1.0.0.jar \
  --spring.profiles.active=cli \
  --package orders-package.json \
  --source-password "secret" \
  --marklogic-password "secret"

# Package with overrides (ML connection from the package, custom directory)
java -jar DataMigrationFramework-1.0.0.jar \
  --spring.profiles.active=cli \
  --package orders-package.json \
  --marklogic-connection "ML Staging" \
  --directory "/staging/orders/" \
  --source-password "secret" \
  --marklogic-password "secret"

# Apply a server-side transform on ingest with parameters
java -jar DataMigrationFramework-1.0.0.jar \
  --spring.profiles.active=cli \
  --project "Orders Migration" \
  --marklogic-connection "ML Production" \
  --transform my-transform \
  --transform-param mode=strict \
  --transform-param version=2

# Discover what's available before running
java -jar DataMigrationFramework-1.0.0.jar --spring.profiles.active=cli --list-projects
java -jar DataMigrationFramework-1.0.0.jar --spring.profiles.active=cli --list-connections
java -jar DataMigrationFramework-1.0.0.jar --spring.profiles.active=cli --list-ml-connections
```

### Pre-encrypted Passwords

The `--source-password` and `--marklogic-password` flags accept either plaintext or an already-encrypted `ENC:...` value (produced by the backend's AES-256-GCM key). The encryption service is idempotent — values with the `ENC:` prefix are stored as-is and decrypted on read.

> **Note:** `ENC:` values are tied to the local encryption key at `~/.DataMigrationFramework/encryption.key` and are **not portable between machines**.

### Progress Display

While a migration runs, a live progress bar is rendered in the terminal:

```
[==============================>          ]  73%  14600 / 20000  487 docs/s  [30s]
```

On completion:
```
✓ Migration completed successfully!
  14600 documents written in 30s
```

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Migration completed successfully |
| `1` | Migration failed, was cancelled, or a CLI argument error occurred |

---

## Migration Packages

A migration package is a single portable JSON file bundling a project definition and its associated connection configurations. **Passwords are never included in packages.**

### What's in a Package

```json
{
  "packageVersion": "1.0",
  "exportedAt": "2026-04-03T12:00:00Z",
  "project": { ... },
  "sourceConnection": { "id": "...", "name": "Oracle XE", "connection": { ... } },
  "marklogicConnection": { "id": "...", "name": "ML Prod", "connection": { ... } }
}
```

### Export (UI)

In the **Open Project** modal, click the download icon next to any project. An inline picker lets you optionally select a source connection and/or MarkLogic connection to bundle. Click **Download** to save `{project-name}-package.json`.

### Import (UI)

Click **Import Package** in the footer of the Open Project modal. Drag-and-drop or browse to a package file. The modal previews the contents and lets you supply passwords for any bundled connections before importing.

### Export (API)

```
GET /v1/packages/export/{projectId}
    ?sourceConnectionId=<name or UUID>      (optional)
    &marklogicConnectionId=<name or UUID>   (optional)
```

Returns the package as a `Content-Disposition: attachment` JSON file named `{project-name}-package.json`.

### Import (API)

```
POST /v1/packages/import
     ?sourcePassword=<plaintext or ENC:...>      (optional)
     &marklogicPassword=<plaintext or ENC:...>   (optional)

Body: migration package JSON
```

Returns an `ImportResult` describing what was created vs. what already existed, plus any warnings (e.g. connections imported without a password).

### Import Behaviour

- Entities (project, connections) are matched by **ID**.
- If an entity with the same ID already exists, it is **left untouched** — importing never overwrites existing data.
- Connections imported without a password are saved with a blank password; update them in the Connections panel before running a migration.

---

## REST API

### Projects
```
POST   /v1/projects              Save project
GET    /v1/projects              List all projects
GET    /v1/projects/{id}         Get project by ID
DELETE /v1/projects/{id}         Delete project
```

### Database Connections
```
POST   /v1/connections           Save connection
GET    /v1/connections           List connections
GET    /v1/connections/{name}    Get connection
PUT    /v1/connections/{name}    Update connection
DELETE /v1/connections/{name}    Delete connection
POST   /v1/connections/test      Test connection (inline credentials)
POST   /v1/connections/{id}/test Test saved connection
```

### Schema Analysis
```
POST   /v1/schemas               Analyze DB schema (returns tables, columns, FKs)
GET    /v1/health                Health check
```

### Document Generation (Preview)
```
POST   /v1/projects/{id}/generate/preview        XML document preview
POST   /v1/projects/{id}/generate/json/preview   JSON document preview
```
Returns up to 100 sample documents built from live RDBMS data.

### XML Schema Generation
```
POST   /v1/xml-schema/generate   Generate an XSD from the project's XML mapping definition
```

### MarkLogic Connections
```
POST   /v1/marklogic/connections           Save ML connection
GET    /v1/marklogic/connections           List ML connections
GET    /v1/marklogic/connections/{name}    Get ML connection
PUT    /v1/marklogic/connections/{name}    Update ML connection
DELETE /v1/marklogic/connections/{name}    Delete ML connection
POST   /v1/marklogic/connections/test      Test ML connection
POST   /v1/marklogic/connections/{id}/test Test saved ML connection
```

### Migration Packages
```
GET    /v1/packages/export/{projectId}   Export project as a package file (download)
POST   /v1/packages/import               Import a package (create project + connections)
```

### Data Migration
```
POST   /v1/migration/jobs                  Start migration job
GET    /v1/migration/jobs                  List jobs
GET    /v1/migration/jobs/{id}/progress    Job progress (poll)
GET    /v1/migration/jobs/{id}/events      Job progress (SSE stream)
DELETE /v1/migration/jobs/{id}             Delete completed job
```

---

## Project Structure

```
src/main/java/com/nativelogix/data/migration/framework/
├── Application.java
├── cli/
│   ├── MigrationCliCommand.java   # Picocli command (args, progress bar, --package support)
│   └── MigrationCliRunner.java    # Spring CommandLineRunner (cli profile only)
├── controller/                    # REST controllers (one per domain)
│   ├── PackageController.java     # Export / import migration packages
│   └── ...
├── model/
│   ├── project/                   # Project and mapping data models
│   ├── relational/                # RDBMS schema models
│   ├── generate/                  # Document generation request/response
│   ├── migration/                 # Batch job models
│   ├── MigrationPackage.java      # Portable project + connection bundle
│   └── ImportResult.java          # Result returned after package import
├── service/
│   ├── JDBCConnectionService.java
│   ├── SchemaService.java
│   ├── MarkLogicConnectionService.java
│   ├── PackageService.java        # Export (strip passwords) / import (skip existing by ID)
│   ├── PasswordEncryptionService.java
│   ├── generate/                  # XmlGenerationService, JsonGenerationService,
│   │                              # SqlQueryBuilder, JoinResolver, XmlSchemaGenerationService,
│   │                              # XmlDocumentBuilder, JsonDocumentBuilder,
│   │                              # JavaScriptFunctionExecutor (Rhino)
│   └── migration/                 # Spring Batch config, CursorPipelineTasklet,
│                                  # MigrationJobService, MigrationMetrics
├── repository/                    # Filesystem-backed repositories
└── util/                          # Case conversion, type mapping
```

## Data Storage

All data is stored under `~/.datamigrationframework/`:

```
~/.datamigrationframework/
├── projects/                  # Project definitions
├── connections/               # Source DB connections (passwords AES-encrypted)
├── marklogic-connections/     # MarkLogic connections (passwords AES-encrypted)
└── migration-jobs/            # Job execution records
```

The encryption key is stored at `~/.DataMigrationFramework/encryption.key`. If this file is deleted or the application is moved to a new machine, saved passwords will no longer decrypt — re-enter and save each connection to re-encrypt with the new key.

## Key Implementation Notes

- **Cursor pipeline**: Each migration job opens one streaming JDBC cursor (no OFFSET paging) and fans out to N configurable worker threads via a `BlockingQueue`. Workers batch-fetch child rows and build documents concurrently before handing off to a shared DMSDK `WriteBatcher`. This replaces the previous parallel-partition approach and eliminates the need for row-count-based partitioning.
- **Spring Batch**: H2 in-memory metadata DB; jobs are triggered via `POST /v1/migration/jobs` or the CLI — not auto-started on launch. Each job is a single `TaskletStep` wrapping `CursorPipelineTasklet`.
- **Custom field functions**: Set `mappingType: CUSTOM` on any column and provide a JavaScript expression in `customFunction`. The expression is evaluated by Mozilla Rhino with the current row available as a map of column values. Works for both XML and JSON mappings.
- **XML serialization**: `XmlDocumentBuilder` uses `StringBuilder`-based serialization rather than DOM + `TransformerFactory`, eliminating per-document service-loader overhead. `DocumentBuilderFactory` is a static singleton; `DocumentBuilder` is pooled per thread via `ThreadLocal`.
- **XSD generation**: `XmlSchemaGenerationService` derives an XSD from a project's XML mapping definition, available via `POST /v1/xml-schema/generate`.
- **Project IDs**: New projects always receive a UUID — name-as-ID fallback has been removed. Duplicate project names return HTTP 409.
- **Atomic file writes**: `FileSystemProjectRepository` writes to a `.tmp` file then atomically renames to prevent partial reads during concurrent access.
- **Metrics**: `MigrationMetrics` exposes Micrometer counters and timers (`ml.docs.written`, `ml.write.errors`, `ml.write.duration`) scraped by Prometheus via `/actuator/prometheus`.
- **SPA routing**: `SpaController` forwards unmatched paths to `index.html` so the React app handles client-side routing.
- **Credentials**: Passwords are AES-256-GCM encrypted at rest and never returned to the frontend. The encryption service is idempotent — values already prefixed with `ENC:` are stored unchanged.
- **Ingest transforms**: An optional MarkLogic REST `ServerTransform` can be attached per job. When a transform name is specified, it is applied to every document via `WriteBatcher.withTransform()` on the DMSDK path, and via the `DocumentManager.write(writeSet, transform)` overload on the synchronous fallback path. Transform parameters (key=value) are passed through to the transform function.
- **Package portability**: Migration packages contain no passwords. `ENC:` values supplied at import time are machine-local and not transferable between installations.
- **CORS**: Configured for `http://localhost:5176` and `http://localhost:3000` (dev servers).

---

## Observability

A local monitoring stack (Prometheus + Grafana) is bundled in the `docker/` directory.

### Running the Stack

```bash
cd docker
docker compose up -d
```

| Service | URL |
|---------|-----|
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |

Prometheus scrapes the Spring Boot actuator endpoint at `http://host.docker.internal:9390/actuator/prometheus` every 5 seconds. The pre-provisioned Grafana dashboard shows documents written, write errors, and write latency in real time.

> **Note:** The Spring Boot app runs outside Docker. Start the JAR first, then bring up the compose stack.

## Docs

Feature planning documents are in `docs/`:
- `json-plan.md` — JSON document generation design
- `kafka-cdc-plan.md` — Kafka CDC integration plan
- `xml-generation-plan.md` — XML document generation strategy
- `dhf-export-plan.md` — MarkLogic DHF export functionality plan
