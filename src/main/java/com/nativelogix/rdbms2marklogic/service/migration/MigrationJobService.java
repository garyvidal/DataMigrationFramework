package com.nativelogix.rdbms2marklogic.service.migration;

import com.nativelogix.rdbms2marklogic.model.migration.DeploymentJob;
import com.nativelogix.rdbms2marklogic.model.migration.DeploymentJobStatus;
import com.nativelogix.rdbms2marklogic.model.migration.MigrationProgress;
import com.nativelogix.rdbms2marklogic.model.migration.MigrationRequest;
import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.model.SavedMarkLogicConnection;
import com.nativelogix.rdbms2marklogic.repository.FileSystemDeploymentJobRepository;
import com.nativelogix.rdbms2marklogic.repository.FileSystemProjectRepository;
import com.nativelogix.rdbms2marklogic.service.JDBCConnectionService;
import com.nativelogix.rdbms2marklogic.service.MarkLogicConnectionService;
import com.nativelogix.rdbms2marklogic.service.PasswordEncryptionService;
import com.nativelogix.rdbms2marklogic.service.generate.JoinResolver;
import com.nativelogix.rdbms2marklogic.service.generate.JsonDocumentBuilder;
import com.nativelogix.rdbms2marklogic.service.generate.SqlQueryBuilder;
import com.nativelogix.rdbms2marklogic.service.generate.XmlDocumentBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MigrationJobService {

    private static final int CHUNK_SIZE = 50;

    private final FileSystemProjectRepository projectRepository;
    private final FileSystemDeploymentJobRepository jobRepository;
    private final MarkLogicConnectionService markLogicConnectionService;
    private final JDBCConnectionService jdbcConnectionService;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JoinResolver joinResolver;
    private final XmlDocumentBuilder xmlDocumentBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;
    private final PasswordEncryptionService passwordEncryptionService;
    private final JobRepository batchJobRepository;
    private final PlatformTransactionManager transactionManager;

    @Qualifier("asyncJobLauncher")
    private final JobLauncher asyncJobLauncher;

    // ── Start a new migration job ─────────────────────────────────────────────

    public DeploymentJob startJob(MigrationRequest request) {
        // 1. Resolve project
        Project project = projectRepository.findById(request.getProjectId())
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + request.getProjectId()));

        // 2. Resolve MarkLogic connection (lookup by id, fall back to name)
        SavedMarkLogicConnection mlConn = resolveMarkLogicConnection(request.getMarklogicConnectionId());

        // 3. Count total records for progress tracking
        long totalRecords = countRootRecords(project);

        // 4. Create job record
        DeploymentJob job = new DeploymentJob();
        job.setId(UUID.randomUUID().toString());
        job.setProjectId(project.getId());
        job.setProjectName(project.getName());
        job.setMarklogicConnectionId(mlConn.getId());
        job.setMarklogicConnectionName(mlConn.getName());
        job.setDirectoryPath(request.getDirectoryPath());
        job.setCollections(request.getCollections() != null ? request.getCollections() : List.of());
        job.setStatus(DeploymentJobStatus.PENDING);
        job.setTotalRecords(totalRecords);
        job.setCreated(OffsetDateTime.now());
        jobRepository.save(job);

        // 5. Build and launch async Spring Batch job
        MigrationJobContext ctx = new MigrationJobContext(job, project, mlConn,
                request.getDirectoryPath(), job.getCollections());
        launchBatchJob(job.getId(), ctx);

        return job;
    }

    // ── Progress query ────────────────────────────────────────────────────────

    public Optional<MigrationProgress> getProgress(String jobId) {
        return jobRepository.findById(jobId).map(job -> {
            MigrationProgress progress = new MigrationProgress();
            progress.setJobId(job.getId());
            progress.setStatus(job.getStatus());
            progress.setTotalRecords(job.getTotalRecords());
            progress.setProcessedRecords(job.getProcessedRecords());
            progress.setErrorMessage(job.getErrorMessage());
            progress.setErrors(job.getErrors());

            if (job.getStartTime() != null) {
                OffsetDateTime end = job.getEndTime() != null ? job.getEndTime() : OffsetDateTime.now();
                progress.setElapsedSeconds(ChronoUnit.SECONDS.between(job.getStartTime(), end));
            }
            return progress;
        });
    }

    public List<DeploymentJob> getAllJobs() {
        return jobRepository.findAll();
    }

    public void deleteJob(String jobId) {
        jobRepository.delete(jobId);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void launchBatchJob(String jobId, MigrationJobContext ctx) {
        RdbmsDocumentReader reader = new RdbmsDocumentReader(
                ctx, jdbcConnectionService, sqlQueryBuilder, joinResolver,
                xmlDocumentBuilder, jsonDocumentBuilder);

        MarkLogicDocumentWriter writer = new MarkLogicDocumentWriter(ctx, passwordEncryptionService);

        Step step = new StepBuilder("migrationStep-" + jobId, batchJobRepository)
                .<DocumentBuildResult, DocumentBuildResult>chunk(CHUNK_SIZE, transactionManager)
                .reader(reader)
                .writer(writer)
                .listener(buildStepListener(jobId))
                .build();

        Job batchJob = new JobBuilder("migrationJob-" + jobId, batchJobRepository)
                .start(step)
                .build();

        JobParameters params = new JobParametersBuilder()
                .addString("jobId", jobId)
                .addLong("startTime", System.currentTimeMillis())
                .toJobParameters();

        try {
            asyncJobLauncher.run(batchJob, params);
        } catch (Exception e) {
            log.error("Failed to launch batch job {}: {}", jobId, e.getMessage(), e);
            jobRepository.findById(jobId).ifPresent(job -> {
                job.setStatus(DeploymentJobStatus.FAILED);
                job.setErrorMessage("Failed to launch: " + e.getMessage());
                job.setEndTime(OffsetDateTime.now());
                jobRepository.save(job);
            });
        }
    }

    private StepExecutionListener buildStepListener(String jobId) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                jobRepository.findById(jobId).ifPresent(job -> {
                    job.setStatus(DeploymentJobStatus.RUNNING);
                    job.setStartTime(OffsetDateTime.now());
                    jobRepository.save(job);
                });
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                jobRepository.findById(jobId).ifPresent(job -> {
                    job.setProcessedRecords(stepExecution.getWriteCount());
                    if (stepExecution.getStatus() == BatchStatus.COMPLETED) {
                        job.setStatus(DeploymentJobStatus.COMPLETED);
                    } else if (stepExecution.getStatus() == BatchStatus.FAILED) {
                        job.setStatus(DeploymentJobStatus.FAILED);
                        stepExecution.getFailureExceptions().stream().findFirst()
                                .ifPresent(e -> job.setErrorMessage(e.getMessage()));
                    }
                    job.setEndTime(OffsetDateTime.now());
                    jobRepository.save(job);
                });
                return stepExecution.getExitStatus();
            }
        };
    }

    /** Periodic progress update listener — called after each chunk write */
    private ItemWriteListener<DocumentBuildResult> buildWriteListener(String jobId) {
        return new ItemWriteListener<>() {
            @Override
            public void afterWrite(Chunk<? extends DocumentBuildResult> items) {
                jobRepository.findById(jobId).ifPresent(job -> {
                    job.setProcessedRecords(job.getProcessedRecords() + items.size());
                    jobRepository.save(job);
                });
            }
        };
    }

    private long countRootRecords(Project project) {
        try {
            String mappingType = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";
            String schema, table;
            if ("JSON".equalsIgnoreCase(mappingType) && project.getMapping().getJsonDocumentModel() != null
                    && project.getMapping().getJsonDocumentModel().getRoot() != null) {
                var root = project.getMapping().getJsonDocumentModel().getRoot();
                schema = root.getSourceSchema();
                table = root.getSourceTable();
            } else if (project.getMapping() != null && project.getMapping().getDocumentModel() != null
                    && project.getMapping().getDocumentModel().getRoot() != null) {
                var root = project.getMapping().getDocumentModel().getRoot();
                schema = root.getSourceSchema();
                table = root.getSourceTable();
            } else {
                return 0L;
            }

            String qualifiedTable = (schema != null && !schema.isBlank())
                    ? "\"" + schema + "\".\"" + table + "\""
                    : "\"" + table + "\"";

            String connName = project.getConnectionName();
            var savedConn = jdbcConnectionService.getConnection(connName)
                    .orElseThrow(() -> new IllegalArgumentException("Connection not found: " + connName));
            try (var jdbcConn = jdbcConnectionService.openJdbcConnection(savedConn.getConnection());
                 var stmt = jdbcConn.createStatement();
                 var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable)) {
                if (rs.next()) return rs.getLong(1);
            }
        } catch (Exception e) {
            log.warn("Could not count root records: {}", e.getMessage());
        }
        return 0L;
    }

    private SavedMarkLogicConnection resolveMarkLogicConnection(String idOrName) {
        return markLogicConnectionService.getAllConnections().stream()
                .filter(c -> idOrName.equals(c.getId()) || idOrName.equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("MarkLogic connection not found: " + idOrName));
    }
}
