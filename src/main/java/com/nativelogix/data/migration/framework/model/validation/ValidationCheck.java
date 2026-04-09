package com.nativelogix.data.migration.framework.model.validation;

/**
 * A single pre-flight check result.
 *
 * @param checkId  Stable identifier (e.g. "SOURCE_CONNECTIVITY").
 * @param category Logical grouping: "CONNECTIVITY", "MAPPING", or "SECURITY".
 * @param label    Human-readable check name shown in the UI.
 * @param status   PASS, WARN, or FAIL.
 * @param detail   Extended message (e.g. list of missing roles). Null when not applicable.
 * @param hint     Optional remediation suggestion. Null when not applicable.
 */
public record ValidationCheck(
        String checkId,
        String category,
        String label,
        CheckStatus status,
        String detail,
        String hint
) {
    public static ValidationCheck pass(String checkId, String category, String label) {
        return new ValidationCheck(checkId, category, label, CheckStatus.PASS, null, null);
    }

    public static ValidationCheck warn(String checkId, String category, String label, String detail) {
        return new ValidationCheck(checkId, category, label, CheckStatus.WARN, detail, null);
    }

    public static ValidationCheck warn(String checkId, String category, String label, String detail, String hint) {
        return new ValidationCheck(checkId, category, label, CheckStatus.WARN, detail, hint);
    }

    public static ValidationCheck fail(String checkId, String category, String label, String detail) {
        return new ValidationCheck(checkId, category, label, CheckStatus.FAIL, detail, null);
    }

    public static ValidationCheck fail(String checkId, String category, String label, String detail, String hint) {
        return new ValidationCheck(checkId, category, label, CheckStatus.FAIL, detail, hint);
    }
}
