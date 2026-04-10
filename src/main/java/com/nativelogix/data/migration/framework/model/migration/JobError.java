package com.nativelogix.data.migration.framework.model.migration;

/**
 * Records a single document-level write failure during a migration job.
 * Captured in {@link DeploymentJob#getErrors()} for use by the retry pipeline.
 */
public class JobError {

    /** The MarkLogic document URI that failed to write. */
    private String documentUri;
    /** Human-readable failure reason from the WriteBatcher batch failure callback. */
    private String failureReason;
    /** Source primary key value (stringified) — used by RetryTasklet to re-fetch the row. */
    private String sourcePk;

    public JobError() {}

    public JobError(String documentUri, String failureReason, String sourcePk) {
        this.documentUri   = documentUri;
        this.failureReason = failureReason;
        this.sourcePk      = sourcePk;
    }

    public String getDocumentUri()   { return documentUri; }
    public void setDocumentUri(String documentUri) { this.documentUri = documentUri; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    public String getSourcePk()      { return sourcePk; }
    public void setSourcePk(String sourcePk) { this.sourcePk = sourcePk; }
}
