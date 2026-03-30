package com.nativelogix.rdbms2marklogic.model.migration;

import java.util.List;

public class MigrationRequest {

    private String projectId;
    private String sourceConnectionId;
    private String marklogicConnectionId;
    private String directoryPath;
    private List<String> collections;

    public MigrationRequest() {}

    public String getProjectId() { return projectId; }
    public void setProjectId(String projectId) { this.projectId = projectId; }

    public String getSourceConnectionId() { return sourceConnectionId; }
    public void setSourceConnectionId(String sourceConnectionId) { this.sourceConnectionId = sourceConnectionId; }

    public String getMarklogicConnectionId() { return marklogicConnectionId; }
    public void setMarklogicConnectionId(String marklogicConnectionId) { this.marklogicConnectionId = marklogicConnectionId; }

    public String getDirectoryPath() { return directoryPath; }
    public void setDirectoryPath(String directoryPath) { this.directoryPath = directoryPath; }

    public List<String> getCollections() { return collections; }
    public void setCollections(List<String> collections) { this.collections = collections; }
}
