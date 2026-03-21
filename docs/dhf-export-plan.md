# DHF Export Plan

## Goal

Add Export/Generate feature that produces a DHF 5+ project folder structure (zip download) from the current `ProjectData`.

## Output Structure

```
{project-name}-dhf/
├── gradle.properties
├── build.gradle
├── entities/
│   └── {EntityName}.entity.json          ← one per RootElement/RootObject
├── flows/
│   └── {EntityName}Flow.flow.json        ← one per root entity
└── steps/
    ├── ingestion/
    │   └── {EntityName}Ingest.step.json
    ├── mapping/
    │   └── {EntityName}Mapping.step.json
    └── custom/
        └── {EntityName}Joiner.step.json  ← only if entity has synthetic joins
            (+ src/main/ml-modules/root/custom-modules/custom/{EntityName}Joiner/main.sjs)
```

## Artifact Rules

### Entity Definitions (`entities/{E}.entity.json`)
- One per `RootElement` / `RootObject` table mapping
- Columns → ES properties with type mapping:

| Source type | ES type |
|-------------|---------|
| `xs:string`, JSON `string` | `string` |
| `xs:integer` | `integer` |
| `xs:long` | `long` |
| `xs:decimal`, JSON `number` | `decimal` |
| `xs:boolean`, JSON `boolean` | `boolean` |
| `xs:date` | `date` |
| `xs:dateTime` | `dateTime` |
| `xs:hexBinary` | `string` |

- `Elements`/`Array` children → ES array of objects (nested in `definitions`)
- `InlineElement`/`InlineObject` children → ES nested object property

### Flow (`flows/{E}Flow.flow.json`)
- Steps: `1` = Ingest, `2` = Mapping, `3` = Joiner (if synthetic joins exist)

### Ingestion Step (`steps/ingestion/{E}Ingest.step.json`)
- `stepDefinitionName: "default-ingestion"` with `options.sourceSchema/sourceTable/connectionName`

### Mapping Step (`steps/mapping/{E}Mapping.step.json`)
- `stepDefinitionName: "entity-services-mapping"`
- `properties` block: each column mapping → `"<xmlName|jsonKey>": { "sourcedFrom": "<sourceColumn>" }`
- `CUSTOM` columns → `sourcedFrom` set to the custom function body with a `// CUSTOM — review` comment

### Custom Joiner Step (synthetic joins)
- `steps/custom/{E}Joiner.step.json`
- `src/main/ml-modules/root/custom-modules/custom/{E}Joiner/main.sjs` — SJS stub with join conditions as comments

### gradle.properties
```
mlHost=localhost
mlPort=8811
mlUsername=admin
mlPassword=admin
mlAppName={projectName}
hubDhs=false
hubSsl=false
mlDHFVersion=5.8.1
```
Note: gradle-8.9 wrapper in use.

## Backend Implementation

New files:
- `service/dhf/DhfExportService.java` — builds all artifacts as `Map<path, String>`, zips into byte[]
- `controller/DhfExportController.java` — `GET /v1/projects/{id}/export/dhf` → `application/zip`

Uses Jackson `ObjectMapper` directly (no new model classes needed). Depends on existing `ProjectRepository` and `CaseConverter`.

Response headers:
- `Content-Type: application/zip`
- `Content-Disposition: attachment; filename="{projectName}-dhf.zip"`

## Frontend Implementation

Modified files:
- `services/ProjectService.ts` — add `exportDhf(projectId)` → triggers download via `<a download>` blob trick
- `components/SchemaView/SchemaToolbar.tsx` — add "Export DHF" button, gated on `hasActiveProject`
- `components/SchemaView/SchemaView.tsx` — wire the new toolbar prop through

## Edge Cases

| Case | Handling |
|------|---------|
| Multi-table root (FK joins) | All `Elements` children included as arrays in entity |
| `CUSTOM` column/table mappings | Included with `// CUSTOM — review` comment in step file |
| Synthetic joins on non-root tables | Included in joiner step for whichever root entity owns the source table |
| `wrapInParent` XML wrapper | Ignored (DHF has no equivalent) |
| `mappingType = BOTH` | Prefer JSON model if present, fall back to XML model for entity/mapping generation |
| `NamingCase` | Applied to entity property names and `sourcedFrom` keys via `CaseConverter` |
