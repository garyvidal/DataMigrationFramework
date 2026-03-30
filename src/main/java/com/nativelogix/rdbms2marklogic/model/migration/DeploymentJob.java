package com.nativelogix.rdbms2marklogic.model.migration;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class DeploymentJob {

    private String id;
    private String projectId;
    private String projectName;
    private String sourceConnectionId;
    private String sourceConnectionName;
    private String marklogicConnectionId;
    private String marklogicConnectionName;
    private String directoryPath;
    private List<String> collections = new ArrayList<>();
    private DeploymentJobStatus status = DeploymentJobStatus.PENDING;
    private long totalRecords;
    private long processedRecords;
    private String errorMessage;
    private List<String> errors = new ArrayList<>();
    private OffsetDateTime created;
    private OffsetDateTime startTime;
    private OffsetDateTime endTime;

    public DeploymentJob() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getProjectId() { return projectId; }
    public void setProjectId(String projectId) { this.projectId = projectId; }

    public String getProjectName() { return projectName; }
    public void setProjectName(String projectName) { this.projectName = projectName; }

    public String getSourceConnectionId() { return sourceConnectionId; }
    public void setSourceConnectionId(String sourceConnectionId) { this.sourceConnectionId = sourceConnectionId; }

    public String getSourceConnectionName() { return sourceConnectionName; }
    public void setSourceConnectionName(String sourceConnectionName) { this.sourceConnectionName = sourceConnectionName; }

    public String getMarklogicConnectionId() { return marklogicConnectionId; }
    public void setMarklogicConnectionId(String marklogicConnectionId) { this.marklogicConnectionId = marklogicConnectionId; }

    public String getMarklogicConnectionName() { return marklogicConnectionName; }
    public void setMarklogicConnectionName(String marklogicConnectionName) { this.marklogicConnectionName = marklogicConnectionName; }

    public String getDirectoryPath() { return directoryPath; }
    public void setDirectoryPath(String directoryPath) { this.directoryPath = directoryPath; }

    public List<String> getCollections() { return collections; }
    public void setCollections(List<String> collections) { this.collections = collections != null ? collections : new ArrayList<>(); }

    public DeploymentJobStatus getStatus() { return status; }
    public void setStatus(DeploymentJobStatus status) { this.status = status; }

    public long getTotalRecords() { return totalRecords; }
    public void setTotalRecords(long totalRecords) { this.totalRecords = totalRecords; }

    public long getProcessedRecords() { return processedRecords; }
    public void setProcessedRecords(long processedRecords) { this.processedRecords = processedRecords; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors != null ? errors : new ArrayList<>(); }

    public OffsetDateTime getCreated() { return created; }
    public void setCreated(OffsetDateTime created) { this.created = created; }

    public OffsetDateTime getStartTime() { return startTime; }
    public void setStartTime(OffsetDateTime startTime) { this.startTime = startTime; }

    public OffsetDateTime getEndTime() { return endTime; }
    public void setEndTime(OffsetDateTime endTime) { this.endTime = endTime; }
}
