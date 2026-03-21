# CDC + Kafka Integration Plan

## Overview

Integrate Change Data Capture (CDC) into rdbms2marklogic using Kafka as the event broker.
The feature adds a Deployment Wizard UI, a Kafka Connect component that generates connector
properties files, and a Bulk Load capability for initial data snapshots.

---

## Architecture

```
RDBMS  ──►  Kafka Connect Source  ──►  Kafka Topic  ──►  Kafka Connect Sink  ──►  MarkLogic
              (Debezium CDC /                                (MarkLogic Sink
               JDBC Source)                                  Connector)
                    ▲                                              │
                    │                                              ▼
              Project Mapping                             Documents in ML
           (schema → topics →                          (collections, URIs,
            collections)                                format from mapping)
```

---

## Data Model Extensions

### Frontend — `app/src/services/ProjectService.ts`

Add to `ProjectSettings`:
```typescript
kafkaConfig?: KafkaDeploymentConfig;
```

New types:

```typescript
export type KafkaConnectorMode = 'DEBEZIUM_CDC' | 'JDBC_SOURCE';
export type KafkaSecurity     = 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL';

export interface KafkaBrokerConfig {
  bootstrapServers: string;
  schemaRegistryUrl?: string;
  security: KafkaSecurity;
  saslMechanism?: string;
  saslJaasConfig?: string;
  sslTruststoreLocation?: string;
  sslTruststorePassword?: string;
  sslKeystoreLocation?: string;
  sslKeystorePassword?: string;
  connectRestUrl: string;
}

export interface KafkaSourceConnectorConfig {
  connectorMode: KafkaConnectorMode;
  // Debezium-specific
  debeziumPlugin?: string;          // pgoutput | decoderbufs | wal2json
  debeziumSlotName?: string;
  snapshotMode?: string;            // initial | never | schema_only
  dbziumDatabaseServerId?: string;  // MySQL server-id
  // JDBC Source-specific
  jdbcPollIntervalMs?: number;
  timestampColumn?: string;
  incrementingColumn?: string;
  // Common
  tasksMax?: number;
}

export interface KafkaSinkConnectorConfig {
  marklogicConnectionId?: string;
  batchSize?: number;
  idStrategy?: string;              // uuid | hash | documentUri
  permCollections?: string[];
  permRoles?: string[];
  outputFormat?: 'JSON' | 'XML';
  uriPrefix?: string;
  uriSuffix?: string;
}

export interface KafkaTopicMapping {
  schemaName: string;
  tableName: string;
  topicName: string;
  collections: string[];
  directory?: string;
  outputFormat?: 'JSON' | 'XML';
}

export interface KafkaDeploymentConfig {
  id?: string;
  broker: KafkaBrokerConfig;
  sourceConnector: KafkaSourceConnectorConfig;
  sinkConnector: KafkaSinkConnectorConfig;
  topicMappings: KafkaTopicMapping[];
  lastDeployed?: string;
  generatedFiles?: Record<string, string>;
}
```

Also extend `ProjectData`:
```typescript
kafkaConfig?: KafkaDeploymentConfig;
```

### Backend — `model/project/Project.java`

```java
private KafkaDeploymentConfig kafkaConfig;
```

`FileSystemProjectRepository` uses `FAIL_ON_UNKNOWN_PROPERTIES = false`, so existing stored
projects load safely with `kafkaConfig = null`.

---

## New Files

### Frontend

```
app/src/
├── services/
│   └── KafkaService.ts
└── components/Kafka/
    ├── DeployKafkaWizard.tsx        # 5-step modal (entry point)
    ├── Step1BrokerConfig.tsx        # Broker/security form + Test Connection
    ├── Step2SourceConnector.tsx     # Debezium CDC vs JDBC Source options
    ├── Step3SinkConnector.tsx       # MarkLogic sink config
    ├── Step4TopicMapping.tsx        # table → topic → ML collection grid
    ├── Step5ReviewDeploy.tsx        # Generated file preview + deploy buttons
    ├── KafkaPropertiesPreview.tsx   # Syntax-colored .properties file viewer
    └── BulkLoadPanel.tsx           # Snapshot trigger + progress polling
```

### Backend

```
src/main/java/com/nativelogix/rdbms2marklogic/
├── model/kafka/
│   ├── KafkaBrokerConfig.java
│   ├── KafkaSourceConnectorConfig.java
│   ├── KafkaSinkConnectorConfig.java
│   ├── KafkaTopicMapping.java
│   └── KafkaDeploymentConfig.java
├── service/kafka/
│   ├── KafkaConnectPropertiesGenerator.java
│   ├── KafkaConnectDeploymentService.java
│   └── BulkLoadService.java
└── controller/
    └── KafkaDeploymentController.java
```

---

## Existing Files to Modify

### Frontend

| File | Change |
|------|--------|
| `app/src/services/ProjectService.ts` | Add all Kafka types; extend `ProjectData` |
| `app/src/App.tsx` | Add `showKafkaWizard` state, wire `DeployKafkaWizard` |
| `app/src/components/Layout/Header.tsx` | Add "Deploy to Kafka" button (teal accent), `onDeployKafka` prop |
| `app/src/components/SchemaView/SchemaToolbar.tsx` | Optional: per-project Kafka button gated on `hasActiveProject` |

### Backend

| File | Change |
|------|--------|
| `model/project/Project.java` | Add `KafkaDeploymentConfig kafkaConfig` field |

---

## 5-Step Deployment Wizard

The wizard follows the `CreateProjectWizard.tsx` structural pattern exactly:
fixed overlay, `bg-slate-800` container, `StepIndicator` at top, scrollable body, Back/Next footer.

### Step 1 — Broker Config
- Bootstrap servers (comma-separated)
- Kafka Connect REST URL
- Schema Registry URL (optional)
- Security protocol (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL)
- Conditional SASL fields (mechanism, JAAS config)
- Conditional SSL fields (keystore/truststore paths)
- **Test Connection** button → `GET {connectRestUrl}/connectors`

### Step 2 — Source Connector
- Mode toggle: **Debezium CDC** / **JDBC Source**
- Debezium section (when CDC):
  - Logical decoding plugin (pgoutput / decoderbufs / wal2json — adapted to DB type)
  - Replication slot name (auto-derived from project name)
  - Snapshot mode (initial / never / schema_only)
  - Server ID (MySQL only)
  - Tasks max
- JDBC Source section (when JDBC):
  - Poll interval (ms)
  - Incrementing column
  - Timestamp column
  - Tasks max

### Step 3 — Sink Connector
- MarkLogic connection (dropdown from saved ML connections)
- Batch size (default 100)
- Output format (JSON / XML — defaults to project mapping type)
- URI strategy (uuid / hash / documentUri)
- URI prefix / suffix
- Default collections (pill input)
- Default permission roles

### Step 4 — Topic Mapping

Editable grid — one row per table in the project:

| Schema | Table | Kafka Topic | ML Collections | Directory |
|--------|-------|-------------|----------------|-----------|

- Debezium auto-name: `{serverName}.{schema}.{table}`
- JDBC auto-name: `{prefix}.{tableName}`
- Bulk-edit: "Apply prefix to all" button

### Step 5 — Review & Deploy

**Left/top — Generated Files panel:**
- Tabs: `worker.properties` / `source-connector.properties` / `sink-connector.properties`
- Each tab shows file content in `KafkaPropertiesPreview` (key=value colorized, monospace)
- Download per-file button + Download All as ZIP

**Right/bottom — Deploy panel:**
- Connect API status indicator
- Deploy Source Connector button
- Deploy Sink Connector button
- Connector status badges (Idle / Running / Failed)
- Bulk Load section (see below)

---

## REST API Endpoints

All endpoints follow existing `@CrossOrigin` + `ResponseEntity<>` controller pattern.

```
POST   /v1/projects/{id}/kafka/config               # save KafkaDeploymentConfig
GET    /v1/projects/{id}/kafka/config               # load KafkaDeploymentConfig
POST   /v1/projects/{id}/kafka/generate             # returns generated .properties files
POST   /v1/projects/{id}/kafka/connect/test         # test Connect REST reachability
POST   /v1/projects/{id}/kafka/deploy/source        # deploy source connector via Connect API
POST   /v1/projects/{id}/kafka/deploy/sink          # deploy sink connector via Connect API
GET    /v1/projects/{id}/kafka/status               # connector running status
POST   /v1/projects/{id}/kafka/snapshot             # trigger initial full-table snapshot
GET    /v1/projects/{id}/kafka/snapshot/{jobId}     # poll snapshot progress
```

---

## Generated Properties File Templates

### `worker.properties`
```properties
bootstrap.servers=<bootstrapServers>
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
# Security — appended conditionally when security != PLAINTEXT
security.protocol=<security>
# SSL — appended when SSL or SASL_SSL
ssl.truststore.location=<sslTruststoreLocation>
ssl.truststore.password=<sslTruststorePassword>
ssl.keystore.location=<sslKeystoreLocation>
ssl.keystore.password=<sslKeystorePassword>
# SASL — appended when SASL_PLAINTEXT or SASL_SSL
sasl.mechanism=<saslMechanism>
sasl.jaas.config=<saslJaasConfig>
```

### `source-connector.properties` (Debezium / Postgres)
```properties
name=<projectName>-cdc-source
connector.class=io.debezium.connector.postgresql.PostgresConnector
tasks.max=<tasksMax>
database.hostname=<conn.host>
database.port=<conn.port>
database.user=<conn.username>
database.password=<conn.password>
database.dbname=<conn.database>
database.server.name=<dbziumDatabaseServerId>
plugin.name=<debeziumPlugin>
slot.name=<debeziumSlotName>
snapshot.mode=<snapshotMode>
table.include.list=<schema.table,schema.table,...>
topic.prefix=<dbziumDatabaseServerId>
```

### `source-connector.properties` (JDBC Source)
```properties
name=<projectName>-jdbc-source
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
tasks.max=<tasksMax>
connection.url=<jdbcUrl>
connection.user=<conn.username>
connection.password=<conn.password>
mode=incrementing
incrementing.column.name=<incrementingColumn>
timestamp.column.name=<timestampColumn>
poll.interval.ms=<jdbcPollIntervalMs>
table.whitelist=<schema.table,schema.table,...>
topic.prefix=<topicPrefix>.
```

### `sink-connector.properties`
```properties
name=<projectName>-marklogic-sink
connector.class=com.marklogic.kafka.connect.sink.MarkLogicSinkConnector
tasks.max=1
ml.connection.host=<mlConn.host>
ml.connection.port=<mlConn.port>
ml.connection.username=<mlConn.username>
ml.connection.password=<mlConn.password>
ml.connection.type=<mlConn.authType>
ml.connection.simple-ssl=<mlConn.useSSL>
ml.document.format=<outputFormat>
ml.document.uriPrefix=<uriPrefix>
ml.document.uriSuffix=<uriSuffix>
ml.document.collections=<collections>
ml.document.permissions=<permRoles>
topics=<topic1,topic2,...>
```

**Generator method signature:**
```java
// KafkaConnectPropertiesGenerator.java
public Map<String, String> generate(Project project, KafkaDeploymentConfig kafkaConfig)
// Returns: { "worker.properties" -> "...", "source-connector.properties" -> "...", "sink-connector.properties" -> "..." }
```

---

## Bulk Load / Initial Snapshot

`BulkLoadService.java` performs a full table scan before CDC streaming begins:

1. Uses `JDBCConnectionService` to read all rows from selected tables (same as `XmlGenerationService` pattern)
2. Applies project `ProjectMapping` transformations (reuses XML/JSON generation logic)
3. Writes documents to MarkLogic via `/v1/documents` REST API (batched)
4. Tracks progress in `ConcurrentHashMap<String, SnapshotJobState>` keyed by UUID jobId

`BulkLoadPanel.tsx` UI:
- Table checkbox list (all selected by default)
- Batch size input (default 1000)
- "Start Snapshot" button
- Progress bar polling `getSnapshotStatus(jobId)` every 2s
- Status line: "Processing 4,521 / 12,000 rows..."

---

## `KafkaService.ts` — Frontend API Layer

```typescript
// app/src/services/KafkaService.ts
export const saveKafkaConfig       = (projectId: string, config: KafkaDeploymentConfig) => Promise<KafkaDeploymentConfig>
export const getKafkaConfig        = (projectId: string) => Promise<KafkaDeploymentConfig>
export const testKafkaConnect      = (projectId: string, connectRestUrl: string) => Promise<KafkaConnectTestResult>
export const generateKafkaFiles    = (projectId: string) => Promise<Record<string, string>>
export const downloadKafkaZip      = (projectId: string, projectName: string) => Promise<void>
export const deploySourceConnector = (projectId: string, config: KafkaDeploymentConfig) => Promise<DeployResult>
export const deploySinkConnector   = (projectId: string, config: KafkaDeploymentConfig) => Promise<DeployResult>
export const getKafkaStatus        = (projectId: string) => Promise<KafkaDeployStatus>
export const triggerSnapshot       = (projectId: string, tables?: string[]) => Promise<{ jobId: string }>
export const getSnapshotStatus     = (projectId: string, jobId: string) => Promise<SnapshotJob>
```

---

## Phased Delivery

### Phase 1 — MVP: Config + File Generation
**Goal:** Configure Kafka settings per-project and download `.properties` files. No live Kafka needed.

Frontend:
1. Add Kafka types to `ProjectService.ts`
2. Create `KafkaService.ts` (save/load/generate/download only)
3. Build `DeployKafkaWizard` Steps 1–4
4. Build `Step5ReviewDeploy` with file preview tabs + download (no deploy buttons)
5. Build `KafkaPropertiesPreview`
6. Wire "Deploy to Kafka" button in `Header.tsx` / `App.tsx`

Backend:
1. Add `model/kafka/` classes
2. Extend `Project.java`
3. Implement `KafkaConnectPropertiesGenerator`
4. Add `KafkaDeploymentController` endpoints: `POST/GET /kafka/config`, `POST /kafka/generate`

---

### Phase 2 — Live Deploy: Connect REST Integration
**Goal:** Test Connect reachability and deploy connectors from the wizard.

Frontend:
1. Add deploy/status functions to `KafkaService.ts`
2. Add Test Connection to Step 1
3. Add Deploy buttons + status badges to Step 5

Backend:
1. Add `KafkaConnectDeploymentService` (RestTemplate calls to Connect REST API)
2. Add endpoints: `POST /kafka/connect/test`, `POST /kafka/deploy/source`, `POST /kafka/deploy/sink`, `GET /kafka/status`

---

### Phase 3 — Bulk Load: Initial Snapshot
**Goal:** Full table scan → MarkLogic documents before CDC streaming starts.

Backend:
1. Implement `BulkLoadService` (reuses JDBC + generation service patterns)
2. Add endpoints: `POST /kafka/snapshot`, `GET /kafka/snapshot/{jobId}`

Frontend:
1. Add snapshot functions to `KafkaService.ts`
2. Build `BulkLoadPanel` with progress polling

---

### Phase 4 — Polish
- Kafka status section in right panel
- Connector restart/pause/resume
- Task-level error reporting
- Re-generate prompt when project mappings change after initial deploy

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Properties generation is backend-only | Credentials are resolved server-side; ML connection looked up by ID |
| Wizard state is a single `kafkaConfig` object | Follows existing `ConfigDialog` controlled-component pattern; no Redux needed |
| Connector naming: `{projectName}-cdc-source` / `{projectName}-marklogic-sink` | Derived from `project.getName()`, lowercased, spaces → hyphens |
| Topic naming (Debezium): `{serverName}.{schema}.{table}` | Matches Debezium's automatic topic naming convention |
| Topic naming (JDBC): `{prefix}.{tableName}` | Matches JDBC Source Connector default |
| No new npm dependencies | Uses existing `react-icons/fa` + Tailwind; `KafkaPropertiesPreview` is regex-based colorizer |
| Assumed sink connector: `com.marklogic.kafka.connect.sink.MarkLogicSinkConnector` | Open-source MarkLogic Kafka Connector; Step 3 exposes a connector class override field |
| Bulk load job tracking: in-memory `ConcurrentHashMap` | Acceptable for single-user local tool; note as tech debt for multi-user |
