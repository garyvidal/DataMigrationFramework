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
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.lang.NonNull;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
    private final MigrationMetrics migrationMetrics;

    @Qualifier("asyncJobLauncher")
    private final JobLauncher asyncJobLauncher;

    @Qualifier("migrationDocBuilderExecutor")
    private final TaskExecutor docBuilderExecutor;

    // ── Pipeline tuning (application.properties) ──────────────────────────────
    @Value("${migration.marklogic.batcher.batch-size:500}")
    private int batcherBatchSize;

    @Value("${migration.marklogic.batcher.thread-count:8}")
    private int batcherThreadCount;

    @Value("${migration.pipeline.worker-threads:0}")   // 0 = auto (CPU count)
    private int workerThreadCount;

    @Value("${migration.pipeline.queue-capacity:8}")
    private int queueCapacity;

    @Value("${migration.dryrun.sample-size:100}")
    private int dryRunSampleSize;

    // ── SSE emitter registry ──────────────────────────────────────────────────

    /** Live processed-record counters for in-flight jobs; removed on job completion. */
    private final ConcurrentHashMap<String, AtomicLong> liveProcessedCounters = new ConcurrentHashMap<>();

    /** Active SSE emitters keyed by jobId; supports multiple subscribers per job. */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<SseEmitter>> jobEmitters = new ConcurrentHashMap<>();
    /**
     * Epoch-millis timestamp of the last SSE push per job.
     * Rate-limits SSE events to one every {@value #SSE_PUSH_INTERVAL_MS} ms.
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
        job.setDryRun(request.isDryRun());
        job.setTransformName(request.getTransformName());
        job.setTransformParams(request.getTransformParams());
        job.setTotalRecords(totalRecords);
        job.setCreated(OffsetDateTime.now());

        if (request.isDryRun()) {
            // Dry run: persist the job as immediately completed — no documents are written.
            job.setStatus(DeploymentJobStatus.COMPLETED);
            job.setProcessedRecords(0);
            job.setStartTime(OffsetDateTime.now());
            job.setEndTime(OffsetDateTime.now());
            jobRepository.save(job);
            log.info("Dry run job {} completed — {} source records counted, no documents written.",
                    job.getId(), totalRecords);
            return job;
        }

        job.setStatus(DeploymentJobStatus.PENDING);
        jobRepository.save(job);

        MigrationJobContext ctx = new MigrationJobContext(job, project, sourceConn, mlConn,
                request.getDirectoryPath(), effectiveSecurity,
                request.getTransformName(), request.getTransformParams());
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

    /**
     * Retries a FAILED or PARTIALLY_COMPLETED job by launching a new full migration job
     * with the same project, connections, security config, and transform settings.
     * Returns the new job. The original job record is unchanged.
     */
    public DeploymentJob retryJob(String jobId) {
        DeploymentJob original = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
        if (original.getStatus() != DeploymentJobStatus.FAILED
                && original.getStatus() != DeploymentJobStatus.PARTIALLY_COMPLETED) {
            throw new IllegalStateException(
                    "Job " + jobId + " cannot be retried — status is " + original.getStatus());
        }
        MigrationRequest request = new MigrationRequest();
        request.setProjectId(original.getProjectId());
        request.setSourceConnectionId(original.getSourceConnectionId());
        request.setMarklogicConnectionId(original.getMarklogicConnectionId());
        request.setDirectoryPath(original.getDirectoryPath());
        request.setSecurityConfig(original.getSecurityConfig());
        request.setTransformName(original.getTransformName());
        request.setTransformParams(original.getTransformParams());
        request.setDryRun(false);
        log.info("Retrying job {} — launching new job from same parameters", jobId);
        return startJob(request);
    }

    /**
     * Runs a dry-run sample against an existing job's project configuration.
     * Exercises the full pipeline (cursor → child fetch → document build) on
     * {@code migration.dryrun.sample-size} rows (default 100) without writing to MarkLogic.
     */
    public com.nativelogix.data.migration.framework.model.migration.DryRunReport runDryRunSample(String jobId) {
        DeploymentJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        com.nativelogix.data.migration.framework.model.project.Project project =
                projectRepository.findById(job.getProjectId())
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + job.getProjectId()));

        SavedMarkLogicConnection mlConn = resolveMarkLogicConnection(job.getMarklogicConnectionId());
        SavedConnection sourceConn     = resolveSourceConnection(job.getSourceConnectionId(), project);
        long totalRecords              = countRootRecords(project, sourceConn);

        MigrationJobContext ctx = new MigrationJobContext(job, project, sourceConn, mlConn,
                job.getDirectoryPath(), job.getSecurityConfig(),
                job.getTransformName(), job.getTransformParams());

        DryRunTasklet tasklet = new DryRunTasklet(ctx, jdbcConnectionService, sqlQueryBuilder,
                joinResolver, xmlDocumentBuilder, jsonDocumentBuilder, dryRunSampleSize);

        log.info("Running dry-run sample for job {} — sample size {}", jobId, dryRunSampleSize);
        return tasklet.run(totalRecords);
    }

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
     * Builds and launches a Spring Batch job backed by a single {@link CursorPipelineTasklet}.
     *
     * <p>The tasklet opens one streaming JDBC cursor (no OFFSET), feeds batches into a
     * {@link java.util.concurrent.BlockingQueue}, and runs N worker threads that concurrently
     * batch-fetch child rows and build documents before adding them to a shared DMSDK
     * {@link com.marklogic.client.datamovement.WriteBatcher}.</p>
     */
    private void launchBatchJob(String jobId, MigrationJobContext ctx, long totalRecords) {
        AtomicLong processedCounter = new AtomicLong(0);
        AtomicReference<OffsetDateTime> startTimeRef = new AtomicReference<>();
        jobLastPushMs.put(jobId, new AtomicLong(0));
        liveProcessedCounters.put(jobId, processedCounter);

        int workers = workerThreadCount > 0
                ? workerThreadCount
                : Math.min(16, Runtime.getRuntime().availableProcessors());

        CursorPipelineTasklet tasklet = new CursorPipelineTasklet(
                ctx,
                jdbcConnectionService,
                sqlQueryBuilder,
                joinResolver,
                xmlDocumentBuilder,
                jsonDocumentBuilder,
                passwordEncryptionService,
                markLogicSecurityService,
                migrationMetrics,
                docBuilderExecutor,
                workers,
                batcherBatchSize,
                batcherThreadCount,
                queueCapacity,
                processedCounter);

        // Wrap progress pushes in a StepExecutionListener so the SSE ticker still fires.
        Step step = new StepBuilder("migrationStep-" + jobId, batchJobRepository)
                .tasklet(tasklet, transactionManager)
                .listener(buildSseStepListener(jobId, processedCounter, totalRecords, startTimeRef))
                .build();

        Job batchJob = new JobBuilder("migrationJob-" + jobId, batchJobRepository)
                .start(step)
                .listener(buildJobListener(jobId, processedCounter, startTimeRef, tasklet))
                .build();

        JobParameters params = new JobParametersBuilder()
                .addString("jobId", jobId)
                .addLong("startTime", System.currentTimeMillis())
                .toJobParameters();

        log.info("Job {} launching cursor pipeline — {} worker threads, batcher threads={}",
                jobId, workers, batcherThreadCount);
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

    // ── Listeners ─────────────────────────────────────────────────────────────

    /**
     * Job-level listener: marks the job RUNNING on start, COMPLETED/FAILED on finish,
     * flushes SSE emitters, and cleans up per-job state.
     */
    private JobExecutionListener buildJobListener(String jobId, AtomicLong processedCounter,
                                                   AtomicReference<OffsetDateTime> startTimeRef,
                                                   CursorPipelineTasklet tasklet) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(@NonNull JobExecution jobExecution) {
                migrationMetrics.jobStarted();
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
                migrationMetrics.jobFinished();
                jobRepository.findById(jobId).ifPresent(job -> {
                    boolean anyFailed = jobExecution.getStepExecutions().stream()
                            .anyMatch(se -> se.getStatus() == BatchStatus.FAILED);
                    var docErrors = tasklet.getFailedDocErrors();
                    if (anyFailed) {
                        job.setStatus(DeploymentJobStatus.FAILED);
                        jobExecution.getStepExecutions().stream()
                                .filter(se -> se.getStatus() == BatchStatus.FAILED)
                                .flatMap(se -> se.getFailureExceptions().stream())
                                .findFirst()
                                .ifPresent(e -> job.setErrorMessage(e.getMessage()));
                    } else if (!docErrors.isEmpty()) {
                        job.setStatus(DeploymentJobStatus.PARTIALLY_COMPLETED);
                    } else {
                        job.setStatus(DeploymentJobStatus.COMPLETED);
                    }
                    if (!docErrors.isEmpty()) {
                        job.setErrors(docErrors);
                    }
                    job.setProcessedRecords(processedCounter.get());
                    job.setEndTime(OffsetDateTime.now());
                    jobRepository.save(job);
                });

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
     * Step-level listener that fires periodic SSE progress pushes.
     * The tasklet updates {@code processedCounter} directly; this listener polls it
     * on a CAS-gated 2-second interval so the UI stays responsive during long runs.
     */
    private StepExecutionListener buildSseStepListener(String jobId, AtomicLong processedCounter,
                                                        long totalRecords,
                                                        AtomicReference<OffsetDateTime> startTimeRef) {
        return new StepExecutionListener() {
            // Background ticker thread — cancelled in afterStep.
            private volatile java.util.concurrent.ScheduledFuture<?> ticker;
            private final java.util.concurrent.ScheduledExecutorService scheduler =
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                        Thread t = new Thread(r, "sse-ticker-" + jobId);
                        t.setDaemon(true);
                        return t;
                    });

            @Override
            public void beforeStep(@NonNull StepExecution stepExecution) {
                ticker = scheduler.scheduleAtFixedRate(() -> {
                    AtomicLong lastPush = jobLastPushMs.get(jobId);
                    if (lastPush == null) return;
                    long now = System.currentTimeMillis();
                    long prev = lastPush.get();
                    if (now - prev >= SSE_PUSH_INTERVAL_MS && lastPush.compareAndSet(prev, now)) {
                        pushSseEvent(jobId, "progress", processedCounter.get(), totalRecords, startTimeRef);
                    }
                }, SSE_PUSH_INTERVAL_MS, SSE_PUSH_INTERVAL_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

            @Override
            public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
                if (ticker != null) ticker.cancel(false);
                scheduler.shutdown();
                return stepExecution.getExitStatus();
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
