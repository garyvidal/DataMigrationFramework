package com.nativelogix.data.migration.framework.model.validation;

import java.time.Instant;
import java.util.List;

/**
 * Aggregated result of all pre-flight validation checks.
 * {@link #canProceed} is true when no FAIL checks are present.
 */
public class ValidationReport {

    private final List<ValidationCheck> checks;
    private final boolean canProceed;
    private final boolean hasWarnings;
    private final Instant evaluatedAt;

    public ValidationReport(List<ValidationCheck> checks) {
        this.checks      = List.copyOf(checks);
        this.canProceed  = checks.stream().noneMatch(c -> c.status() == CheckStatus.FAIL);
        this.hasWarnings = checks.stream().anyMatch(c -> c.status() == CheckStatus.WARN);
        this.evaluatedAt = Instant.now();
    }

    public List<ValidationCheck> getChecks()    { return checks; }
    public boolean isCanProceed()               { return canProceed; }
    public boolean isHasWarnings()              { return hasWarnings; }
    public Instant getEvaluatedAt()             { return evaluatedAt; }
}
