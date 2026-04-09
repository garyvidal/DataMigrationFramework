package com.nativelogix.data.migration.framework.model.migration;

import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;

import java.util.List;
import java.util.Map;

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
    /** When true, count source records and validate config but do not write any documents to MarkLogic. */
    private boolean dryRun = false;
    /** Name of a server-side MarkLogic REST transform to apply on ingest (optional). */
    private String transformName;
    /** Named parameters passed to the transform (optional). */
    private Map<String, String> transformParams;

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

    public boolean isDryRun() { return dryRun; }
    public void setDryRun(boolean dryRun) { this.dryRun = dryRun; }

    public String getTransformName() { return transformName; }
    public void setTransformName(String transformName) { this.transformName = transformName; }

    public Map<String, String> getTransformParams() { return transformParams; }
    public void setTransformParams(Map<String, String> transformParams) { this.transformParams = transformParams; }
}
