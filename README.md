# RdbmsToMarkLogic — Backend API

A Spring Boot REST API that powers the RDBMS-to-MarkLogic data migration framework. It introspects relational database schemas, builds XML and JSON document mappings, previews generated documents using live data, and executes large-scale batch migrations to MarkLogic.

## Purpose

This backend enables:
- **Schema analysis** — introspect relational databases (PostgreSQL, Oracle, SQL Server, MySQL) using SchemaCrawler
- **Document generation** — build XML and JSON documents from mapping definitions and live RDBMS data
- **Batch migration** — run Spring Batch jobs to read from RDBMS and write documents to MarkLogic at scale
- **Project persistence** — store and retrieve migration projects, mappings, and connection credentials on the filesystem
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
| JDBC | PostgreSQL 42.7 |
| Credentials | AES encryption at rest |
| Frontend bundling | `frontend-maven-plugin` (Node 22 + Vite) |

## Prerequisites

- Java 23
- Maven 3.8+
- A running PostgreSQL (or other supported) database for schema crawling
- A running MarkLogic server as the migration target

## Getting Started

```bash
# Build everything (Java + React frontend)
mvn clean package

# Run the application
mvn spring-boot:run
# or
java -jar target/RdbmsToMarkLogic-1.0-SNAPSHOT.jar
```

The API starts at **`http://localhost:9390`**.

The React frontend (for production) is bundled into the JAR and served at the root path. During development, the frontend runs separately at `http://localhost:5173`.

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

### Data Migration
```
POST   /v1/migration/jobs                  Start migration job
GET    /v1/migration/jobs                  List jobs
GET    /v1/migration/jobs/{id}/progress   Job progress
DELETE /v1/migration/jobs/{id}             Delete completed job
```

## Project Structure

```
src/main/java/com/nativelogix/rdbms2marklogic/
├── Application.java
├── controller/           # REST controllers (one per domain)
├── model/
│   ├── project/          # Project and mapping data models
│   ├── relational/       # RDBMS schema models
│   ├── generate/         # Document generation request/response
│   └── migration/        # Batch job models
├── service/
│   ├── JDBCConnectionService.java
│   ├── SchemaService.java
│   ├── MarkLogicConnectionService.java
│   ├── generate/         # XmlGenerationService, JsonGenerationService,
│   │                     # SqlQueryBuilder, JoinResolver, document builders
│   └── migration/        # Spring Batch config, reader, writer, job service
├── repository/
│   └── FileSystemProjectRepository.java
└── util/                 # Case conversion, encryption, type mapping
```

## Key Implementation Notes

- **Lombok + Java 23**: Annotation processor path is explicitly configured in `pom.xml` to avoid compatibility issues. If annotation processing fails, use explicit constructors/getters instead.
- **Spring Batch**: H2 in-memory metadata DB; jobs are not auto-started on application launch — they are triggered via `POST /v1/migration/jobs`.
- **SPA routing**: `SpaController` forwards unmatched paths to `index.html` so the React app handles client-side routing.
- **Credentials**: Database and MarkLogic passwords are AES-encrypted at rest; passwords are never returned to the frontend.
- **CORS**: Configured to allow `http://localhost:5173` (Vite dev server) during development.

## Docs

Feature planning documents are in `docs/`:
- `json-plan.md` — JSON document generation design
- `kafka-cdc-plan.md` — Kafka CDC integration plan
- Additional planning docs for DHF export and other features
