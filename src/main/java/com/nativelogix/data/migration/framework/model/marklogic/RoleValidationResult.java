package com.nativelogix.data.migration.framework.model.marklogic;

import java.util.List;

/**
 * Result of a best-effort role validation check against the MarkLogic Management API.
 *
 * @param valid            true only if all roles were confirmed to exist
 * @param missingRoles     roles that were confirmed to NOT exist (404 from Management API)
 * @param unvalidatedRoles roles that could not be checked (auth failure, port blocked, timeout)
 */
public record RoleValidationResult(
        boolean valid,
        List<String> missingRoles,
        List<String> unvalidatedRoles
) {
    public static RoleValidationResult ok() {
        return new RoleValidationResult(true, List.of(), List.of());
    }

    public boolean hasWarnings() {
        return !unvalidatedRoles.isEmpty();
    }
}
