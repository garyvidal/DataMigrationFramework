package com.nativelogix.data.migration.framework.service.migration;

import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.migration.DeploymentJob;
import com.nativelogix.data.migration.framework.model.migration.DeploymentJobStatus;
import com.nativelogix.data.migration.framework.model.migration.MigrationPreviewResult;
import com.nativelogix.data.migration.framework.model.migration.MigrationProgress;
import com.nativelogix.data.migration.framework.model.migration.MigrationRequest;
import com.nativelogix.data.migration.framework.model.migration.TableRowCount;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;
import com.nativelogix.data.migration.framework.repository.FileSystemDeploymentJobRepository;
import com.nativelogix.data.migration.framework.repository.FileSystemProjectRepository;
import com.nativelogix.data.migration.framework.service.JDBCConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicSecurityService;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import com.nativelogix.data.migration.framework.service.generate.JoinResolver;
import com.nativelogix.data.migration.framework.service.generate.JsonDocumentBuilder;
import com.nativelogix.data.migration.framework.service.generate.SqlQueryBuilder;
import com.nativelogix.data.migration.framework.service.generate.XmlDocumentBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.lang.NonNull;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class MigrationJobService {

    /** Documents processed per Spring Batch chunk (write transaction). */
    private static final int  CHUNK_SIZE          = 1000;
    /** Total root-table rows needed to enable parallel partitioning. */
    private static final long PARTITION_THRESHOLD = 10_000L;
    /** Maximum parallel partitions (caps JDBC + ML connections). */
    private static final int  MAX_PARTITIONS      = 8;

    private final FileSystemProjectRepository projectRepository;
    private final FileSystemDeploymentJobRepository jobRepository;
    private final MarkLogicConnectionService markLogicConnectionService;
    private final JDBCConnectionService jdbcConnectionService;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JoinResolver joinResolver;
    private final XmlDocumentBuilder xmlDocumentBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;
    private final PasswordEncryptionService passwordEncryptionService;
    private final MarkLogicSecurityService markLogicSecurityService;
    private final JobRepository batchJobRepository;
    private final PlatformTransactionManager transactionManager;

    @Qualifier("asyncJobLauncher")
    private final JobLauncher asyncJobLauncher;

    @Qualifier("migrationPartitionTaskExecutor")
    private final TaskExecutor migrationPartitionTaskExecutor;

    // ── WriteBatcher configuration (tunable via application.properties) ───────
    @Value("${migration.marklogic.batcher.batch-size:500}")
    private int batcherBatchSize;

    @Value("${migration.marklogic.batcher.thread-count:4}")
    private int batcherThreadCount;

    // ── SSE emitter registry ──────────────────────────────────────────────────

    /** Live processed-record counters for in-flight jobs; removed on job completion. */
    private final ConcurrentHashMap<String, AtomicLong> liveProcessedCounters = new ConcurrentHashMap<>();

    /** Active SSE emitters keyed by jobId; supports multiple subscribers per job. */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<SseEmitter>> jobEmitters = new ConcurrentHashMap<>();
    /**
     * Epoch-millis timestamp of the last SSE push per job.
     * Used by {@link #buildWriteListener} to rate-limit events to one every
     * {@value #SSE_PUSH_INTERVAL_MS} ms regardless of dataset size.
     */
    private final ConcurrentHashMap<String, AtomicLong> jobLastPushMs = new ConcurrentHashMap<>();
    /** Minimum interval between SSE progress events in milliseconds. */
    private static final long SSE_PUSH_INTERVAL_MS = 2_000L;

    /**
     * Creates an SSE emitter for the given job.  Immediately sends the current progress
     * snapshot, then receives push events at each 10% milestone until the job ends.
     * Timeout is set to 30 minutes (longer than any reasonable migration).
     */
    public SseEmitter createSseEmitter(String jobId) {
        SseEmitter emitter = new SseEmitter(30 * 60 * 1000L);
        jobEmitters.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>()).add(emitter);
        jobLastPushMs.computeIfAbsent(jobId, k -> new AtomicLong(0));

        emitter.onCompletion(() -> removeEmitter(jobId, emitter));
        emitter.onTimeout(()    -> removeEmitter(jobId, emitter));
        emitter.onError(ex      -> removeEmitter(jobId, emitter));

        getProgress(jobId).ifPresentOrElse(p -> {
            try {
                emitter.send(SseEmitter.event().name("progress").data(p));
                boolean done = p.getStatus() == DeploymentJobStatus.COMPLETED
                        || p.getStatus() == DeploymentJobStatus.FAILED
                        || p.getStatus() == DeploymentJobStatus.CANCELLED;
                if (done) {
                    emitter.send(SseEmitter.event().name("complete").data(p));
                    emitter.complete();
                }
            } catch (IOException e) {
                log.warn("Failed to send initial SSE event for job {}: {}", jobId, e.getMessage());
                emitter.completeWithError(e);
            }
        }, () -> emitter.completeWithError(new IllegalArgumentException("Job not found: " + jobId)));

        return emitter;
    }

    private void removeEmitter(String jobId, SseEmitter emitter) {
        CopyOnWriteArrayList<SseEmitter> list = jobEmitters.get(jobId);
        if (list != null) list.remove(emitter);
    }

    /**
     * Pushes a progress event to all active emitters for the job.
     * Builds the payload directly from the live counter to reflect in-flight progress.
     */
    private void pushSseEvent(String jobId, String eventName,
                              long processed, long totalRecords,
                              AtomicReference<OffsetDateTime> startTimeRef) {
        CopyOnWriteArrayList<SseEmitter> emitters = jobEmitters.get(jobId);
        if (emitters == null || emitters.isEmpty()) return;

        MigrationProgress p = new MigrationProgress();
        p.setJobId(jobId);
        p.setStatus(DeploymentJobStatus.RUNNING);
        p.setTotalRecords(totalRecords);
        p.setProcessedRecords(processed);
        OffsetDateTime start = startTimeRef.get();
        if (start != null) p.setElapsedSeconds(ChronoUnit.SECONDS.between(start, OffsetDateTime.now()));

        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(p));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    // ── Start a new migration job ─────────────────────────────────────────────

    public DeploymentJob startJob(MigrationRequest request) {
        Project project = projectRepository.findById(request.getProjectId())
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + request.getProjectId()));

        SavedMarkLogicConnection mlConn = resolveMarkLogicConnection(request.getMarklogicConnectionId());
        SavedConnection sourceConn = resolveSourceConnection(request.getSourceConnectionId(), project);

        long totalRecords = countRootRecords(project, sourceConn);

        DeploymentJob job = new DeploymentJob();
        job.setId(UUID.randomUUID().toString());
        job.setProjectId(project.getId());
        job.setProjectName(project.getName());
        job.setSourceConnectionId(sourceConn.getId());
        job.setSourceConnectionName(sourceConn.getName());
        job.setMarklogicConnectionId(mlConn.getId());
        job.setMarklogicConnectionName(mlConn.getName());
        job.setDirectoryPath(request.getDirectoryPath());
        // Merge project-level and job-level security configs (job overrides project).
        MarkLogicSecurityConfig effectiveSecurity = markLogicSecurityService.mergeConfigs(
                project.getSecurityConfig(), request.getSecurityConfig());
        job.setSecurityConfig(effectiveSecurity);
        job.setStatus(DeploymentJobStatus.PENDING);
        job.setTotalRecords(totalRecords);
        job.setCreated(OffsetDateTime.now());
        jobRepository.save(job);

        MigrationJobContext ctx = new MigrationJobContext(job, project, sourceConn, mlConn,
                request.getDirectoryPath(), effectiveSecurity);
        launchBatchJob(job.getId(), ctx, totalRecords);

        return job;
    }

    // ── Progress query ────────────────────────────────────────────────────────

    public Optional<MigrationProgress> getProgress(String jobId) {
        return jobRepository.findById(jobId).map(job -> {
            MigrationProgress progress = new MigrationProgress();
            progress.setJobId(job.getId());
            progress.setStatus(job.getStatus());
            progress.setTotalRecords(job.getTotalRecords());
            AtomicLong liveCounter = liveProcessedCounters.get(job.getId());
            progress.setProcessedRecords(liveCounter != null ? liveCounter.get() : job.getProcessedRecords());
            progress.setErrorMessage(job.getErrorMessage());
            progress.setErrors(job.getErrors());
            if (job.getStartTime() != null) {
                OffsetDateTime end = job.getEndTime() != null ? job.getEndTime() : OffsetDateTime.now();
                progress.setElapsedSeconds(ChronoUnit.SECONDS.between(job.getStartTime(), end));
            }
            return progress;
        });
    }

    public List<DeploymentJob> getAllJobs()              { return jobRepository.findAll(); }
    public Optional<DeploymentJob> getJob(String jobId) { return jobRepository.findById(jobId); }
    public void deleteJob(String jobId)                  { jobRepository.delete(jobId); }

    public Optional<MarkLogicSecurityConfig> getJobSecurity(String jobId) {
        return jobRepository.findById(jobId).map(DeploymentJob::getSecurityConfig);
    }

    public Optional<DeploymentJob> updateJobSecurity(String jobId, MarkLogicSecurityConfig securityConfig) {
        return jobRepository.findById(jobId).map(job -> {
            job.setSecurityConfig(securityConfig);
            return jobRepository.save(job);
        });
    }

    // ── Migration preview ─────────────────────────────────────────────────────

    public MigrationPreviewResult getPreview(String projectId, String connectionId) {
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));

        SavedConnection savedConn = resolveSourceConnection(connectionId, project);

        Map<String, String> tableRoles = new LinkedHashMap<>();
        String mappingType = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";

        if (!"JSON".equalsIgnoreCase(mappingType) && project.getMapping() != null
                && project.getMapping().getDocumentModel() != null) {
            var dm = project.getMapping().getDocumentModel();
            if (dm.getRoot() != null)
                tableRoles.putIfAbsent(tableKey(dm.getRoot().getSourceSchema(), dm.getRoot().getSourceTable()), "root");
            if (dm.getElements() != null)
                dm.getElements().forEach(el -> tableRoles.putIfAbsent(
                        tableKey(el.getSourceSchema(), el.getSourceTable()), "child"));
        }
        if (!"XML".equalsIgnoreCase(mappingType) && project.getMapping() != null
                && project.getMapping().getJsonDocumentModel() != null) {
            var jdm = project.getMapping().getJsonDocumentModel();
            if (jdm.getRoot() != null)
                tableRoles.putIfAbsent(tableKey(jdm.getRoot().getSourceSchema(), jdm.getRoot().getSourceTable()), "root");
            if (jdm.getElements() != null)
                jdm.getElements().forEach(el -> tableRoles.putIfAbsent(
                        tableKey(el.getSourceSchema(), el.getSourceTable()), "child"));
        }

        List<TableRowCount> results = new ArrayList<>();
        long total = 0L;

        try (var jdbcConn = jdbcConnectionService.openJdbcConnection(savedConn.getConnection())) {
            for (Map.Entry<String, String> entry : tableRoles.entrySet()) {
                // tableKey() stores "schema.table" but SQL Server schemas are "database.schema",
                // so split on the LAST dot to correctly separate schema from table name.
                String key     = entry.getKey();
                int lastDot    = key.lastIndexOf('.');
                String schema  = (lastDot > 0) ? key.substring(0, lastDot) : null;
                String table   = (lastDot >= 0) ? key.substring(lastDot + 1) : key;
                if (schema != null && schema.isEmpty()) schema = null;
                String role    = entry.getValue();
                // Quote each part of a potentially dotted schema name (e.g. "AdventureWorks2022.HumanResources")
                String qualified = (schema != null && !schema.isBlank())
                        ? java.util.Arrays.stream(schema.split("\\."))
                            .map(p -> "\"" + p.replace("\"", "\"\"") + "\"")
                            .collect(java.util.stream.Collectors.joining("."))
                            + ".\"" + table.replace("\"", "\"\"") + "\""
                        : "\"" + table.replace("\"", "\"\"") + "\"";

                String whereClause = null;
                if (project.getSchemas() != null) {
                    var dbSchema = project.getSchemas().get(schema != null ? schema : "");
                    if (dbSchema != null && dbSchema.getTables() != null) {
                        var dbTable = dbSchema.getTables().get(table);
                        if (dbTable != null) whereClause = dbTable.getWhereClause();
                    }
                }

                String countSql = "SELECT COUNT(*) FROM " + qualified;
                if (whereClause != null && !whereClause.isBlank()) countSql += " WHERE " + whereClause;

                long count = 0L;
                try (var stmt = jdbcConn.createStatement(); var rs = stmt.executeQuery(countSql)) {
                    if (rs.next()) count = rs.getLong(1);
                } catch (Exception e) {
                    log.warn("Could not count rows for {}: {}", qualified, e.getMessage());
                }
                results.add(new TableRowCount(schema, table, role, count, whereClause));
                if ("root".equals(role)) total += count;
            }
        } catch (Exception e) {
            log.warn("Could not open JDBC connection for preview: {}", e.getMessage());
        }

        return new MigrationPreviewResult(results, total);
    }

    private static String tableKey(String schema, String table) {
        return (schema != null ? schema : "") + "." + (table != null ? table : "");
    }

    // ── Job launching ─────────────────────────────────────────────────────────

    /**
     * Builds and launches the Spring Batch job.  When {@code totalRecords >= PARTITION_THRESHOLD}
     * the root table is split across up to {@value #MAX_PARTITIONS} parallel flows, each backed
     * by its own reader/writer and JDBC connection.
     */
    private void launchBatchJob(String jobId, MigrationJobContext ctx, long totalRecords) {
        int partitionCount = (totalRecords >= PARTITION_THRESHOLD)
                ? Math.min(MAX_PARTITIONS, Runtime.getRuntime().availableProcessors())
                : 1;

        // Shared counter updated by all partitions; used by the job listener for the final count.
        AtomicLong processedCounter = new AtomicLong(0);
        // Populated by beforeJob listener; needed by SSE push to compute elapsed seconds.
        AtomicReference<OffsetDateTime> startTimeRef = new AtomicReference<>();
        // Initialize SSE push-time tracking and live counter for this job.
        jobLastPushMs.put(jobId, new AtomicLong(0));
        liveProcessedCounters.put(jobId, processedCounter);

        List<Flow> flows = new ArrayList<>(partitionCount);
        if (partitionCount == 1) {
            flows.add(buildPartitionFlow(jobId, ctx, 0L, -1L, 0, processedCounter, totalRecords, startTimeRef));
        } else {
            long rowsPerPartition = (totalRecords + partitionCount - 1) / partitionCount;
            for (int i = 0; i < partitionCount; i++) {
                long offset   = (long) i * rowsPerPartition;
                if (offset >= totalRecords) break;
                long pageSize = Math.min(rowsPerPartition, totalRecords - offset);
                flows.add(buildPartitionFlow(jobId, ctx, offset, pageSize, i, processedCounter, totalRecords, startTimeRef));
            }
            log.info("Job {} will run {} parallel partitions (~{} rows each)",
                    jobId, flows.size(), rowsPerPartition);
        }

        Flow mainFlow = (flows.size() == 1)
                ? flows.get(0)
                : new FlowBuilder<SimpleFlow>("splitFlow-" + jobId)
                        .split(migrationPartitionTaskExecutor)
                        .add(flows.toArray(new Flow[0]))
                        .build();

        Job batchJob = new JobBuilder("migrationJob-" + jobId, batchJobRepository)
                .start(mainFlow)
                .end()
                .listener(buildJobListener(jobId, processedCounter, totalRecords, startTimeRef))
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

    /**
     * Builds one partition flow: a step with its own {@link RdbmsDocumentReader} and
     * {@link MarkLogicDocumentWriter} (each maintaining their own JDBC / ML connections).
     */
    private Flow buildPartitionFlow(String jobId, MigrationJobContext ctx,
                                    long offset, long pageSize, int index,
                                    AtomicLong processedCounter, long totalRecords,
                                    AtomicReference<OffsetDateTime> startTimeRef) {
        RdbmsDocumentReader reader = new RdbmsDocumentReader(
                ctx, offset, pageSize,
                jdbcConnectionService, sqlQueryBuilder, joinResolver,
                xmlDocumentBuilder, jsonDocumentBuilder);

        MarkLogicDocumentWriter writer = new MarkLogicDocumentWriter(ctx, passwordEncryptionService,
                markLogicSecurityService, batcherBatchSize, batcherThreadCount);

        Step step = new StepBuilder("migrationStep-" + jobId + "-" + index, batchJobRepository)
                .<DocumentBuildResult, DocumentBuildResult>chunk(CHUNK_SIZE, transactionManager)
                .reader(reader)
                .writer(writer)
                .listener(buildWriteListener(jobId, processedCounter, totalRecords, startTimeRef))
                .build();

        return new FlowBuilder<SimpleFlow>("partitionFlow-" + jobId + "-" + index)
                .start(step)
                .build();
    }

    // ── Listeners ─────────────────────────────────────────────────────────────

    /**
     * Job-level listener: marks the job RUNNING on start, and COMPLETED/FAILED on finish.
     * Using a job listener (rather than a step listener) works correctly for both single and
     * multi-partition runs because it fires once when the entire job finishes.
     */
    private JobExecutionListener buildJobListener(String jobId, AtomicLong processedCounter,
                                                   long totalRecords,
                                                   AtomicReference<OffsetDateTime> startTimeRef) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(@NonNull JobExecution jobExecution) {
                jobRepository.findById(jobId).ifPresent(job -> {
                    job.setStatus(DeploymentJobStatus.RUNNING);
                    OffsetDateTime now = OffsetDateTime.now();
                    job.setStartTime(now);
                    startTimeRef.set(now);
                    jobRepository.save(job);
                });
            }

            @Override
            public void afterJob(@NonNull JobExecution jobExecution) {
                jobRepository.findById(jobId).ifPresent(job -> {
                    boolean anyFailed = jobExecution.getStepExecutions().stream()
                            .anyMatch(se -> se.getStatus() == BatchStatus.FAILED);
                    job.setStatus(anyFailed ? DeploymentJobStatus.FAILED : DeploymentJobStatus.COMPLETED);
                    if (anyFailed) {
                        jobExecution.getStepExecutions().stream()
                                .filter(se -> se.getStatus() == BatchStatus.FAILED)
                                .flatMap(se -> se.getFailureExceptions().stream())
                                .findFirst()
                                .ifPresent(e -> job.setErrorMessage(e.getMessage()));
                    }
                    job.setProcessedRecords(processedCounter.get());
                    job.setEndTime(OffsetDateTime.now());
                    jobRepository.save(job);
                });

                // Push final "complete" event and close all emitters for this job
                liveProcessedCounters.remove(jobId);
                CopyOnWriteArrayList<SseEmitter> emitters = jobEmitters.remove(jobId);
                jobLastPushMs.remove(jobId);
                if (emitters != null && !emitters.isEmpty()) {
                    getProgress(jobId).ifPresent(p -> {
                        for (SseEmitter emitter : emitters) {
                            try {
                                emitter.send(SseEmitter.event().name("complete").data(p));
                                emitter.complete();
                            } catch (Exception ignored) {}
                        }
                    });
                }
            }
        };
    }

    /**
     * Write-level listener: increments the shared counter and pushes an SSE progress event
     * at most once every {@value #SSE_PUSH_INTERVAL_MS} ms.  CAS on the timestamp prevents
     * duplicate pushes when concurrent partition threads both notice the interval has elapsed.
     */
    private ItemWriteListener<DocumentBuildResult> buildWriteListener(
            String jobId, AtomicLong processedCounter, long totalRecords,
            AtomicReference<OffsetDateTime> startTimeRef) {
        return new ItemWriteListener<>() {
            @Override
            public void afterWrite(@NonNull Chunk<? extends DocumentBuildResult> items) {
                long processed = processedCounter.addAndGet(items.size());

                AtomicLong lastPush = jobLastPushMs.get(jobId);
                if (lastPush == null) return;

                long now  = System.currentTimeMillis();
                long prev = lastPush.get();
                if (now - prev >= SSE_PUSH_INTERVAL_MS && lastPush.compareAndSet(prev, now)) {
                    pushSseEvent(jobId, "progress", processed, totalRecords, startTimeRef);
                }
            }
        };
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private long countRootRecords(Project project, SavedConnection sourceConn) {
        try {
            String mappingType = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";
            String schema, table;
            if ("JSON".equalsIgnoreCase(mappingType) && project.getMapping().getJsonDocumentModel() != null
                    && project.getMapping().getJsonDocumentModel().getRoot() != null) {
                var root = project.getMapping().getJsonDocumentModel().getRoot();
                schema = root.getSourceSchema();
                table  = root.getSourceTable();
            } else if (project.getMapping() != null && project.getMapping().getDocumentModel() != null
                    && project.getMapping().getDocumentModel().getRoot() != null) {
                var root = project.getMapping().getDocumentModel().getRoot();
                schema = root.getSourceSchema();
                table  = root.getSourceTable();
            } else {
                return 0L;
            }

            // SchemaCrawler returns SQL Server schema names as "database.schema" (e.g. "AdventureWorks2022.HumanResources").
            // Split on dots and quote each part to produce valid multi-part identifiers.
            String qualifiedTable = (schema != null && !schema.isBlank())
                    ? java.util.Arrays.stream(schema.split("\\."))
                        .map(p -> "\"" + p.replace("\"", "\"\"") + "\"")
                        .collect(java.util.stream.Collectors.joining("."))
                        + ".\"" + table.replace("\"", "\"\"") + "\""
                    : "\"" + table.replace("\"", "\"\"") + "\"";

            String whereClause = null;
            if (project.getSchemas() != null) {
                var dbSchema = project.getSchemas().get(schema);
                if (dbSchema != null && dbSchema.getTables() != null) {
                    var dbTable = dbSchema.getTables().get(table);
                    if (dbTable != null) whereClause = dbTable.getWhereClause();
                }
            }
            String countSql = "SELECT COUNT(*) FROM " + qualifiedTable;
            if (whereClause != null && !whereClause.isBlank()) countSql += " WHERE " + whereClause;

            try (var jdbcConn = jdbcConnectionService.openJdbcConnection(sourceConn.getConnection());
                 var stmt = jdbcConn.createStatement();
                 var rs   = stmt.executeQuery(countSql)) {
                if (rs.next()) return rs.getLong(1);
            }
        } catch (Exception e) {
            log.warn("Could not count root records: {}", e.getMessage());
        }
        return 0L;
    }

    private SavedConnection resolveSourceConnection(String connectionId, Project project) {
        // If a connection was explicitly selected, use it by ID (fall back to name match)
        if (connectionId != null && !connectionId.isBlank()) {
            return jdbcConnectionService.getAllConnections().stream()
                    .filter(c -> connectionId.equals(c.getId()) || connectionId.equals(c.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Source connection not found: " + connectionId));
        }
        // Fall back to the connection stored on the project
        String connName = project.getConnectionName();
        return jdbcConnectionService.getConnection(connName)
                .orElseThrow(() -> new IllegalArgumentException("Connection not found: " + connName));
    }

    private SavedMarkLogicConnection resolveMarkLogicConnection(String idOrName) {
        return markLogicConnectionService.getAllConnections().stream()
                .filter(c -> idOrName.equals(c.getId()) || idOrName.equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("MarkLogic connection not found: " + idOrName));
    }
}
