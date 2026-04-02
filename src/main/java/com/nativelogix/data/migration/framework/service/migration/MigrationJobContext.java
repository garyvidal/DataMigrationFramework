package com.nativelogix.rdbms2marklogic.service.migration;

import com.nativelogix.rdbms2marklogic.model.migration.DeploymentJob;
import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.model.SavedConnection;
import com.nativelogix.rdbms2marklogic.model.SavedMarkLogicConnection;

import java.util.List;

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
    private final List<String> collections;

    public MigrationJobContext(DeploymentJob job,
                                Project project,
                                SavedConnection sourceConnection,
                                SavedMarkLogicConnection marklogicConnection,
                                String directoryPath,
                                List<String> collections) {
        this.job = job;
        this.project = project;
        this.sourceConnection = sourceConnection;
        this.marklogicConnection = marklogicConnection;
        this.directoryPath = directoryPath;
        this.collections = collections;
    }

    public DeploymentJob getJob() { return job; }
    public Project getProject() { return project; }
    public SavedConnection getSourceConnection() { return sourceConnection; }
    public SavedMarkLogicConnection getMarklogicConnection() { return marklogicConnection; }
    public String getDirectoryPath() { return directoryPath; }
    public List<String> getCollections() { return collections; }
}
