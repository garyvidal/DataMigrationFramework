package com.nativelogix.rdbms2marklogic.model.migration;

import java.util.List;

public class MigrationProgress {

    private String jobId;
    private DeploymentJobStatus status;
    private long totalRecords;
    private long processedRecords;
    private long elapsedSeconds;
    private String errorMessage;
    private List<String> errors;

    public MigrationProgress() {}

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public DeploymentJobStatus getStatus() { return status; }
    public void setStatus(DeploymentJobStatus status) { this.status = status; }

    public long getTotalRecords() { return totalRecords; }
    public void setTotalRecords(long totalRecords) { this.totalRecords = totalRecords; }

    public long getProcessedRecords() { return processedRecords; }
    public void setProcessedRecords(long processedRecords) { this.processedRecords = processedRecords; }

    public long getElapsedSeconds() { return elapsedSeconds; }
    public void setElapsedSeconds(long elapsedSeconds) { this.elapsedSeconds = elapsedSeconds; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
}
