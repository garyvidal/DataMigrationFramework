package com.nativelogix.data.migration.framework.model.migration;

import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;

import java.util.List;

public class MigrationRequest {

    private String projectId;
    private String sourceConnectionId;
    private String marklogicConnectionId;
    private String directoryPath;
    /** @deprecated Use {@link #securityConfig} instead. */
    @Deprecated
    private List<String> collections;
    /** Job-level security override. Merged on top of the project's securityConfig at job start. */
    private MarkLogicSecurityConfig securityConfig;

    public MigrationRequest() {}

    public String getProjectId() { return projectId; }
    public void setProjectId(String projectId) { this.projectId = projectId; }

    public String getSourceConnectionId() { return sourceConnectionId; }
    public void setSourceConnectionId(String sourceConnectionId) { this.sourceConnectionId = sourceConnectionId; }

    public String getMarklogicConnectionId() { return marklogicConnectionId; }
    public void setMarklogicConnectionId(String marklogicConnectionId) { this.marklogicConnectionId = marklogicConnectionId; }

    public String getDirectoryPath() { return directoryPath; }
    public void setDirectoryPath(String directoryPath) { this.directoryPath = directoryPath; }

    @SuppressWarnings("DeprecatedIsStillUsed")
    public List<String> getCollections() { return collections; }
    @SuppressWarnings("DeprecatedIsStillUsed")
    public void setCollections(List<String> collections) { this.collections = collections; }

    public MarkLogicSecurityConfig getSecurityConfig() { return securityConfig; }
    public void setSecurityConfig(MarkLogicSecurityConfig securityConfig) { this.securityConfig = securityConfig; }
}
