# MarkLogic Security Integration Plan

## Overview

Add support for configuring MarkLogic document-level security settings during the migration process. This includes permissions, collections, quality, and metadata that will be applied to documents as they are ingested into MarkLogic.

## Goal

Enable users to define security policies at the project level that are automatically applied during migration execution, ensuring documents are created with appropriate access controls and organizational metadata.

---

## Phase 1 — Data Model Extensions

### 1.1 New Security Configuration Types

**`MarkLogicPermission`**
```
roleName: string        // e.g., "data-reader", "admin"
capabilities: string[]  // e.g., ["read"], ["read", "update"]
```

**`MarkLogicSecurityConfig`**
```
permissions: MarkLogicPermission[]
collections: string[]   // collection names to assign
quality?: number        // temporal document quality (optional)
metadata?: object       // additional document metadata
```

### 1.2 Project Model Updates

Add security configuration to the `ProjectData` model:

**`ProjectData` (extended)**
```
...existing fields...
securityConfig?: MarkLogicSecurityConfig  // optional security settings
```

### 1.3 Migration Job Updates

Extend `MigrationJob` to inherit or override project security settings:

**`MigrationJob` (extended)**
```
...existing fields...
securityConfig?: MarkLogicSecurityConfig  // job-specific overrides
```

---

## Phase 2 — Backend Implementation

### 2.1 Security Service

Create `MarkLogicSecurityService` to handle security configuration:

- Validate security settings against MarkLogic connection
- Apply permissions, collections, and metadata during document ingestion
- Handle role/capability resolution and validation

### 2.2 Migration Engine Updates

Modify the migration execution logic to:

1. Load security config from project/job
2. Validate against MarkLogic server capabilities
3. Apply security settings to each document during ingestion
4. Log security application results

### 2.3 API Endpoints

Add REST endpoints for security management:

- `GET /api/projects/{id}/security` — retrieve project security config
- `PUT /api/projects/{id}/security` — update project security config
- `GET /api/migration-jobs/{id}/security` — retrieve job security config
- `PUT /api/migration-jobs/{id}/security` — update job security config

---

## Phase 3 — Frontend Integration

### 3.1 Security Configuration UI

Add security tab to project settings:

- Permissions editor (role + capabilities selector)
- Collections input (tag-based)
- Quality slider/input
- Metadata JSON editor

### 3.2 Migration Job Security

Allow job-level security overrides in migration job creation/editing.

### 3.3 Validation and Preview

- Validate roles exist on MarkLogic server
- Preview applied security settings
- Show security inheritance (project → job)

---

## Phase 4 — Testing and Validation

### 4.1 Unit Tests

- Security config validation
- Permission application logic
- Collection assignment

### 4.2 Integration Tests

- End-to-end migration with security
- Role/capability verification
- Document access testing

### 4.3 Documentation

Update user guides with security configuration examples.

---

## Implementation Notes

- Security settings should be optional (backwards compatible)
- Job-level settings override project settings
- Validate roles against MarkLogic user roles
- Support both XML and JSON document security
- Consider performance impact of security validation during high-volume migrations