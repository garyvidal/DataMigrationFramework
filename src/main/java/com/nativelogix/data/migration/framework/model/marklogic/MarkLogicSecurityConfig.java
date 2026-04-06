package com.nativelogix.data.migration.framework.model.marklogic;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Document-level security settings applied during MarkLogic ingestion.
 * All fields are optional; null/empty fields are silently ignored.
 * <p>
 * When configured at both project and job level, the job-level settings
 * take precedence (see {@code MarkLogicSecurityService.mergeConfigs}).
 */
@Data
public class MarkLogicSecurityConfig {
    /** Role-based permissions to apply to each ingested document. */
    private List<MarkLogicPermission> permissions;
    /** MarkLogic collection URIs to assign to each document. */
    private List<String> collections;
    /** MarkLogic document quality score (higher = ranked higher in search). */
    private Integer quality;
    /** Arbitrary key-value metadata pairs attached to each document. */
    private Map<String, String> metadata;
}
