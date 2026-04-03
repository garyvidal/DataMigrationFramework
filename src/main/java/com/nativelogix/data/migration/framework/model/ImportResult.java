package com.nativelogix.data.migration.framework.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Result returned after importing a {@link MigrationPackage}.
 * Describes what was created vs. what already existed, plus any warnings
 * (e.g. connections imported without a password).
 */
@Data
public class ImportResult {

    // ── Project ───────────────────────────────────────────────────────────────

    private String projectId;
    private String projectName;
    /** True if the project did not previously exist and was created. */
    private boolean projectCreated;

    // ── Source connection ─────────────────────────────────────────────────────

    private String sourceConnectionId;
    private String sourceConnectionName;
    /** True if the source connection did not previously exist and was created. */
    private boolean sourceConnectionCreated;

    // ── MarkLogic connection ──────────────────────────────────────────────────

    private String marklogicConnectionId;
    private String marklogicConnectionName;
    /** True if the MarkLogic connection did not previously exist and was created. */
    private boolean marklogicConnectionCreated;

    // ── Warnings ──────────────────────────────────────────────────────────────

    /**
     * Non-fatal messages, e.g. "Connection imported without a password — update it before running a migration."
     */
    private List<String> warnings = new ArrayList<>();
}
