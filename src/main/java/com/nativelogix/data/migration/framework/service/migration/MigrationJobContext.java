package com.nativelogix.data.migration.framework.service.migration;

import com.nativelogix.data.migration.framework.model.migration.DeploymentJob;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;

import java.util.Map;

/**
 * Immutable context passed through a Spring Batch job execution as a job parameter.
 * Holds everything the reader/processor/writer need without re-fetching.
 */
public class MigrationJobContext {

    private final DeploymentJob job;
    private final Project project;
    private final SavedConnection sourceConnection;
    private final SavedMarkLogicConnection marklogicConnection;
    private final String directoryPath;
    /** Effective security config after merging project + job overrides. May be null (MarkLogic defaults apply). */
    private final MarkLogicSecurityConfig securityConfig;
    /** Optional server-side REST transform name to apply on ingest. Null means no transform. */
    private final String transformName;
    /** Optional named parameters for the transform. */
    private final Map<String, String> transformParams;

    public MigrationJobContext(DeploymentJob job,
                                Project project,
                                SavedConnection sourceConnection,
                                SavedMarkLogicConnection marklogicConnection,
                                String directoryPath,
                                MarkLogicSecurityConfig securityConfig,
                                String transformName,
                                Map<String, String> transformParams) {
        this.job                = job;
        this.project            = project;
        this.sourceConnection   = sourceConnection;
        this.marklogicConnection = marklogicConnection;
        this.directoryPath      = directoryPath;
        this.securityConfig     = securityConfig;
        this.transformName      = transformName;
        this.transformParams    = transformParams;
    }

    public DeploymentJob getJob() { return job; }
    public Project getProject() { return project; }
    public SavedConnection getSourceConnection() { return sourceConnection; }
    public SavedMarkLogicConnection getMarklogicConnection() { return marklogicConnection; }
    public String getDirectoryPath() { return directoryPath; }
    public MarkLogicSecurityConfig getSecurityConfig() { return securityConfig; }
    public String getTransformName() { return transformName; }
    public Map<String, String> getTransformParams() { return transformParams; }
}
