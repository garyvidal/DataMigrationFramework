package com.nativelogix.data.migration.framework.service.migration;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.model.project.*;
import com.nativelogix.data.migration.framework.service.JDBCConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicSecurityService;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import com.nativelogix.data.migration.framework.service.generate.JoinResolver;
import com.nativelogix.data.migration.framework.service.generate.JsonDocumentBuilder;
import com.nativelogix.data.migration.framework.service.generate.SqlQueryBuilder;
import com.nativelogix.data.migration.framework.service.generate.XmlDocumentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.task.TaskExecutor;
import org.springframework.lang.NonNull;

import com.nativelogix.data.migration.framework.model.migration.JobError;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Cursor-driven migration pipeline — replaces the OFFSET/LIMIT partition model.
 *
 * <h3>Architecture</h3>
 * <pre>
 *  [JDBC cursor — 1 thread]
 *       │  batches of BATCH_SIZE root rows
 *       ▼
 *  [BlockingQueue]
 *       │  N worker threads pull batches concurrently
 *       ▼
 *  [Document builders — N threads]
 *       │  each: batch-IN child fetch (own JDBC conn) + build XML/JSON
 *       ▼
 *  [DMSDK WriteBatcher — shared, M HTTP writer threads → MarkLogic]
 * </pre>
 *
 * <h3>Key improvements over the OFFSET/LIMIT model</h3>
 * <ul>
 *   <li>No OFFSET penalty — the cursor streams sequentially from the first row.</li>
 *   <li>Single DB connection for root reads; N separate connections for child fetches.</li>
 *   <li>Document building is parallelized across N threads (was single-threaded per partition).</li>
 *   <li>Inline (2nd-level) children are batch-fetched across the whole root batch,
 *       eliminating the per-row N+1 queries that existed in the previous design.</li>
 *   <li>One shared WriteBatcher keeps the MarkLogic HTTP pipeline fully saturated.</li>
 * </ul>
 */
public class CursorPipelineTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(CursorPipelineTasklet.class);

    /** Root rows per queue slot / child-fetch batch. */
    private static final int BATCH_SIZE = 2_000;
    /** Child tables with ≤ this many rows are fully cached in memory on startup. */
    private static final int LOOKUP_CACHE_THRESHOLD = 10_000;

    // Sentinel: worker threads stop when they dequeue this object.
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final List<Map<String, Object>> POISON_PILL = Collections.unmodifiableList(new ArrayList<>());

    // ── Dependencies ──────────────────────────────────────────────────────────

    private final MigrationJobContext ctx;
    private final JDBCConnectionService jdbcConnectionService;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JoinResolver joinResolver;
    private final XmlDocumentBuilder xmlDocumentBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;
    private final PasswordEncryptionService passwordEncryptionService;
    private final MarkLogicSecurityService securityService;
    private final MigrationMetrics metrics;
    private final TaskExecutor builderExecutor;

    // ── Tuning ────────────────────────────────────────────────────────────────

    private final int workerThreadCount;
    private final int batcherBatchSize;
    private final int batcherThreadCount;
    private final int queueCapacity;

    // ── Progress ──────────────────────────────────────────────────────────────

    /** Written by workers; read by MigrationJobService SSE listener. */
    private final AtomicLong processedCounter;
    private final AtomicReference<Exception> firstError = new AtomicReference<>();

    /** Cap on per-document errors collected; prevents unbounded growth on mass failures. */
    private static final int MAX_DOC_ERRORS = 10_000;
    private final AtomicInteger docErrorCount = new AtomicInteger(0);
    /** Thread-safe list of per-document write failures; exposed to MigrationJobService after execution. */
    private final List<JobError> failedDocErrors = new CopyOnWriteArrayList<>();

    /** Returns document-level write failures collected during execution. */
    public List<JobError> getFailedDocErrors() { return failedDocErrors; }

    public CursorPipelineTasklet(MigrationJobContext ctx,
                                  JDBCConnectionService jdbcConnectionService,
                                  SqlQueryBuilder sqlQueryBuilder,
                                  JoinResolver joinResolver,
                                  XmlDocumentBuilder xmlDocumentBuilder,
                                  JsonDocumentBuilder jsonDocumentBuilder,
                                  PasswordEncryptionService passwordEncryptionService,
                                  MarkLogicSecurityService securityService,
                                  MigrationMetrics metrics,
                                  TaskExecutor builderExecutor,
                                  int workerThreadCount,
                                  int batcherBatchSize,
                                  int batcherThreadCount,
                                  int queueCapacity,
                                  AtomicLong processedCounter) {
        this.ctx                   = ctx;
        this.jdbcConnectionService = jdbcConnectionService;
        this.sqlQueryBuilder       = sqlQueryBuilder;
        this.joinResolver          = joinResolver;
        this.xmlDocumentBuilder    = xmlDocumentBuilder;
        this.jsonDocumentBuilder   = jsonDocumentBuilder;
        this.passwordEncryptionService = passwordEncryptionService;
        this.securityService       = securityService;
        this.metrics               = metrics;
        this.builderExecutor       = builderExecutor;
        this.workerThreadCount     = workerThreadCount;
        this.batcherBatchSize      = batcherBatchSize;
        this.batcherThreadCount    = batcherThreadCount;
        this.queueCapacity         = queueCapacity;
        this.processedCounter      = processedCounter;
    }

    // ── Tasklet entry point ───────────────────────────────────────────────────

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution,
                                @NonNull ChunkContext chunkContext) throws Exception {

        String mappingType = resolveMappingType();
        Project project    = ctx.getProject();

        // ── MarkLogic: shared WriteBatcher ────────────────────────────────────
        DatabaseClient mlClient   = buildMarkLogicClient();
        DataMovementManager dmm   = mlClient.newDataMovementManager();
        DocumentMetadataHandle metadata = securityService.buildMetadataHandle(ctx.getSecurityConfig());

        WriteBatcher batcher = dmm.newWriteBatcher()
                .withBatchSize(batcherBatchSize)
                .withThreadCount(batcherThreadCount)
                .onBatchSuccess(batch -> {
                    int n = batch.getItems().length;
                    metrics.mlDocsWrittenCounter.increment(n);
                     log.debug("WriteBatcher flushed {} docs", n);
                })
                .onBatchFailure((batch, ex) -> {
                    metrics.mlWriteErrorCounter.increment(batch.getItems().length);
                    log.error("WriteBatcher batch failed: {}", ex.getMessage(), ex);
                    firstError.compareAndSet(null, new RuntimeException("MarkLogic write failed: " + ex.getMessage(), ex));
                    String reason = ex.getMessage();
                    for (var item : batch.getItems()) {
                        if (docErrorCount.incrementAndGet() <= MAX_DOC_ERRORS) {
                            failedDocErrors.add(new JobError(item.getTargetUri(), reason, null));
                        }
                    }
                });
        dmm.startJob(batcher);

        // ── Queue connecting producer → workers ───────────────────────────────
        // Capacity bounds memory: queueCapacity * BATCH_SIZE rows in flight at once.
        BlockingQueue<List<Map<String, Object>>> queue = new LinkedBlockingQueue<>(queueCapacity);

        // ── Worker futures ────────────────────────────────────────────────────
        CountDownLatch workerLatch = new CountDownLatch(workerThreadCount);
        for (int i = 0; i < workerThreadCount; i++) {
            final int workerIndex = i;
            builderExecutor.execute(() -> {
                try {
                    runWorker(workerIndex, queue, batcher, metadata, mappingType, project);
                } finally {
                    workerLatch.countDown();
                }
            });
        }

        // ── Producer: stream the JDBC cursor ──────────────────────────────────
        try {
            runProducer(queue, mappingType, project);
        } finally {
            // Poison all workers regardless of producer outcome
            for (int i = 0; i < workerThreadCount; i++) {
                queue.put(POISON_PILL);
            }
        }

        workerLatch.await();

        // ── Flush & teardown ──────────────────────────────────────────────────
        try {
            batcher.flushAndWait();
            dmm.stopJob(batcher);
        } catch (Exception e) {
            log.warn("Error flushing WriteBatcher: {}", e.getMessage());
        }
        mlClient.release();

        Exception err = firstError.get();
        if (err != null) throw err;

        return RepeatStatus.FINISHED;
    }

    // ── Producer ──────────────────────────────────────────────────────────────

    /**
     * Streams the root cursor and enqueues batches of {@value #BATCH_SIZE} rows.
     * Uses a single JDBC connection; no OFFSET — purely forward-only streaming.
     */
    private void runProducer(BlockingQueue<List<Map<String, Object>>> queue,
                              String mappingType, Project project) throws Exception {
        Connection sourceConn = ctx.getSourceConnection().getConnection();
        Connection.ConnectionType dbType = sourceConn.getType() != null
                ? sourceConn.getType() : Connection.ConnectionType.Postgres;

        try (java.sql.Connection jdbcConn = jdbcConnectionService.openJdbcConnection(sourceConn)) {

            String sql = buildFullCursorSql(jdbcConn, mappingType, project, dbType);
            log.info("Job {} cursor SQL: {}", ctx.getJob().getId(), sql);

            long t0 = System.nanoTime();
            try (PreparedStatement stmt = jdbcConn.prepareStatement(
                    sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                stmt.setFetchSize(BATCH_SIZE);
                try (ResultSet rs = stmt.executeQuery()) {
                    metrics.recordNanos(metrics.dbRootCursorOpenTimer, System.nanoTime() - t0);

                    String[] cols = columnNames(rs);
                    List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);
                    while (rs.next()) {
                        batch.add(toMap(rs, cols));
                        if (batch.size() == BATCH_SIZE) {
                            queue.put(batch);           // backpressure: blocks if workers are behind
                            metrics.rowsReadCounter.increment(batch.size());
                            metrics.prefetchBatchSizeHistogram.record(batch.size());
                            batch = new ArrayList<>(BATCH_SIZE);
                        }
                    }
                    if (!batch.isEmpty()) {
                        queue.put(batch);
                        metrics.rowsReadCounter.increment(batch.size());
                        metrics.prefetchBatchSizeHistogram.record(batch.size());
                    }
                }
            }
        }
        log.info("Job {} producer finished — cursor exhausted", ctx.getJob().getId());
    }

    // ── Worker ────────────────────────────────────────────────────────────────

    /**
     * Each worker owns one JDBC connection for child fetches and a lookup-cache pre-loaded
     * at startup. Workers run concurrently; each processes one batch end-to-end before
     * pulling the next.
     */
    private void runWorker(int workerIndex,
                            BlockingQueue<List<Map<String, Object>>> queue,
                            WriteBatcher batcher,
                            DocumentMetadataHandle metadata,
                            String mappingType,
                            Project project) {
        Connection sourceConn = ctx.getSourceConnection().getConnection();

        try (java.sql.Connection jdbcConn = jdbcConnectionService.openJdbcConnection(sourceConn)) {

            // Per-worker lookup cache (small child tables loaded fully into memory)
            WorkerLookupCache lookupCache = new WorkerLookupCache(jdbcConn, mappingType, project);

            // Resolve mapping state once per worker
            WorkerMappingState state = new WorkerMappingState(mappingType, project);

            int batchNumber = 0;
            while (true) {
                List<Map<String, Object>> rootBatch = queue.take();
                if (rootBatch == POISON_PILL) break;
                if (firstError.get() != null) break;  // abort on upstream write failure

                processBatch(rootBatch, ++batchNumber, workerIndex,
                        jdbcConn, batcher, metadata, mappingType, project,
                        state, lookupCache);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Worker {} interrupted", workerIndex);
        } catch (Exception e) {
            log.error("Worker {} fatal error: {}", workerIndex, e.getMessage(), e);
            firstError.compareAndSet(null, e);
        }
    }

    /**
     * Processes one root-row batch:
     * 1. Batch-fetch level-1 children (one IN query per child mapping).
     * 2. Batch-fetch level-2 inline children (one IN query per inline mapping per level-1 row set).
     * 3. Build documents (CPU-bound).
     * 4. Add to the shared WriteBatcher (non-blocking).
     */
    private void processBatch(List<Map<String, Object>> rootBatch,
                               int batchNumber, int workerIndex,
                               java.sql.Connection jdbcConn,
                               WriteBatcher batcher,
                               DocumentMetadataHandle metadata,
                               String mappingType, Project project,
                               WorkerMappingState state,
                               WorkerLookupCache lookupCache) {

        NamingCase casing = project.getSettings() != null ? project.getSettings().getDefaultCasing() : null;
        Format mlFormat   = "JSON".equalsIgnoreCase(mappingType) ? Format.JSON : Format.XML;

        try {
            List<DocumentBuildResult> docs;
            if ("JSON".equalsIgnoreCase(mappingType)) {
                Map<String, Map<String, List<Map<String, Object>>>> level1Data =
                        batchFetchJsonChildren(jdbcConn, state.jsonRootLevelMappings, rootBatch,
                                state.jsonRootMapping, project, lookupCache);

                docs = new ArrayList<>(rootBatch.size());
                int idxBase = 0; // URI index within batch — unique across batches via workerIndex/batchNumber
                for (Map<String, Object> rootRow : rootBatch) {
                    Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData =
                            resolveJsonChildren(jdbcConn, rootRow, state.jsonRootMapping,
                                    state.jsonRootLevelMappings, state.jsonInlinesByParentId,
                                    level1Data, project, lookupCache);

                    long t0 = System.nanoTime();
                    String content = jsonDocumentBuilder.build(state.jsonRootMapping, rootRow, childData, casing);
                    metrics.recordNanos(metrics.jsonBuildTimer, System.nanoTime() - t0);

                    String uri = buildUri(rootRow, workerIndex, batchNumber, idxBase++, mappingType, state);
                    docs.add(new DocumentBuildResult(uri + ".json", content, "JSON"));
                }

            } else {
                Map<String, Map<String, List<Map<String, Object>>>> level1Data =
                        batchFetchXmlChildren(jdbcConn, state.xmlRootLevelMappings, rootBatch,
                                state.xmlRootMapping, project, lookupCache);

                List<XmlNamespace> namespaces = project.getMapping().getNamespaces();
                docs = new ArrayList<>(rootBatch.size());
                int idxBase = 0;
                for (Map<String, Object> rootRow : rootBatch) {
                    Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData =
                            resolveXmlChildren(jdbcConn, rootRow, state.xmlRootMapping,
                                    state.xmlRootLevelMappings, state.xmlInlinesByParentId,
                                    level1Data, project, lookupCache);

                    long t0 = System.nanoTime();
                    String content = xmlDocumentBuilder.build(state.xmlRootMapping, rootRow, childData, casing, namespaces);
                    metrics.recordNanos(metrics.xmlBuildTimer, System.nanoTime() - t0);

                    String uri = buildUri(rootRow, workerIndex, batchNumber, idxBase++, mappingType, state);
                    docs.add(new DocumentBuildResult(uri + ".xml", content, "XML"));
                }
            }

            // Add to WriteBatcher — non-blocking; DMSDK flushes when batcherBatchSize is reached
            long t0 = System.nanoTime();
            for (DocumentBuildResult doc : docs) {
                StringHandle content = new StringHandle(doc.getContent()).withFormat(mlFormat);
                if (metadata != null) {
                    batcher.add(doc.getUri(), metadata, content);
                } else {
                    batcher.add(doc.getUri(), content);
                }
            }
            metrics.recordNanos(metrics.mlWriteTimer, System.nanoTime() - t0);

            long processed = processedCounter.addAndGet(docs.size());
            log.debug("Worker {} batch {} — built & queued {} docs (total {})",
                    workerIndex, batchNumber, docs.size(), processed);

        } catch (Exception e) {
            log.error("Worker {} batch {} failed: {}", workerIndex, batchNumber, e.getMessage(), e);
            firstError.compareAndSet(null, e);
        }
    }

    // ── Level-1 batch child fetch ─────────────────────────────────────────────

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchXmlChildren(
            java.sql.Connection jdbcConn,
            List<XmlTableMapping> children,
            List<Map<String, Object>> rootBatch,
            XmlTableMapping rootMapping,
            Project project,
            WorkerLookupCache lookupCache) {

        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (XmlTableMapping child : children) {
            if (lookupCache.isCached(child.getId())) continue;
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                List<Object> parentValues = rootBatch.stream()
                        .map(r -> r.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (parentValues.isEmpty()) continue;

                String sql = sqlQueryBuilder.buildChildBatchQuery(child, joinPath.childColumn(), parentValues.size());
                long t0 = System.nanoTime();
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < parentValues.size(); i++) stmt.setObject(i + 1, parentValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs, cols);
                            String key = String.valueOf(row.get(joinPath.childColumn()));
                            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                metrics.recordNanos(metrics.dbChildBatchFetchTimer, System.nanoTime() - t0);
                result.put(child.getId(), grouped);
            } catch (Exception e) {
                log.warn("Batch child fetch failed for '{}': {}", child.getXmlName(), e.getMessage());
                result.put(child.getId(), Map.of());
            }
        }
        return result;
    }

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchJsonChildren(
            java.sql.Connection jdbcConn,
            List<JsonTableMapping> children,
            List<Map<String, Object>> rootBatch,
            JsonTableMapping rootMapping,
            Project project,
            WorkerLookupCache lookupCache) {

        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (JsonTableMapping child : children) {
            if (lookupCache.isCached(child.getId())) continue;
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                List<Object> parentValues = rootBatch.stream()
                        .map(r -> r.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (parentValues.isEmpty()) continue;

                String sql = sqlQueryBuilder.buildChildBatchQuery(child, joinPath.childColumn(), parentValues.size());
                long t0 = System.nanoTime();
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < parentValues.size(); i++) stmt.setObject(i + 1, parentValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs, cols);
                            String key = String.valueOf(row.get(joinPath.childColumn()));
                            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                metrics.recordNanos(metrics.dbChildBatchFetchTimer, System.nanoTime() - t0);
                result.put(child.getId(), grouped);
            } catch (Exception e) {
                log.warn("Batch child fetch failed for '{}': {}", child.getJsonName(), e.getMessage());
                result.put(child.getId(), Map.of());
            }
        }
        return result;
    }

    // ── Level-1 child resolution (cache → batch data) ─────────────────────────

    private Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> resolveXmlChildren(
            java.sql.Connection jdbcConn,
            Map<String, Object> rootRow,
            XmlTableMapping rootMapping,
            List<XmlTableMapping> rootLevelMappings,
            Map<String, List<XmlTableMapping>> inlinesByParentId,
            Map<String, Map<String, List<Map<String, Object>>>> level1Data,
            Project project,
            WorkerLookupCache lookupCache) throws Exception {

        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        for (XmlTableMapping child : rootLevelMappings) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                Object parentVal = rootRow.get(joinPath.parentColumn());
                if (parentVal == null) { childData.put(child, List.of()); continue; }
                String key = String.valueOf(parentVal);

                List<Map<String, Object>> rows;
                if (lookupCache.isCached(child.getId())) {
                    rows = lookupCache.lookup(child.getId(), joinPath.childColumn(), key);
                } else {
                    rows = level1Data.getOrDefault(child.getId(), Map.of()).getOrDefault(key, List.of());
                }
                childData.put(child, toXmlMappedRows(jdbcConn, child, rows, inlinesByParentId, project, lookupCache));
            } catch (Exception e) {
                log.warn("Child join failed for '{}': {}", child.getXmlName(), e.getMessage());
                childData.put(child, List.of());
            }
        }
        return childData;
    }

    private Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> resolveJsonChildren(
            java.sql.Connection jdbcConn,
            Map<String, Object> rootRow,
            JsonTableMapping rootMapping,
            List<JsonTableMapping> rootLevelMappings,
            Map<String, List<JsonTableMapping>> inlinesByParentId,
            Map<String, Map<String, List<Map<String, Object>>>> level1Data,
            Project project,
            WorkerLookupCache lookupCache) throws Exception {

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        for (JsonTableMapping child : rootLevelMappings) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                Object parentVal = rootRow.get(joinPath.parentColumn());
                if (parentVal == null) { childData.put(child, List.of()); continue; }
                String key = String.valueOf(parentVal);

                List<Map<String, Object>> rows;
                if (lookupCache.isCached(child.getId())) {
                    rows = lookupCache.lookup(child.getId(), joinPath.childColumn(), key);
                } else {
                    rows = level1Data.getOrDefault(child.getId(), Map.of()).getOrDefault(key, List.of());
                }
                childData.put(child, toJsonMappedRows(jdbcConn, child, rows, inlinesByParentId, project, lookupCache));
            } catch (Exception e) {
                log.warn("Child join failed for '{}': {}", child.getJsonName(), e.getMessage());
                childData.put(child, List.of());
            }
        }
        return childData;
    }

    // ── Level-2 inline child resolution (batch-fetched, fixes N+1) ───────────

    private List<XmlDocumentBuilder.MappedRow> toXmlMappedRows(
            java.sql.Connection jdbcConn,
            XmlTableMapping parentMapping,
            List<Map<String, Object>> rows,
            Map<String, List<XmlTableMapping>> inlinesByParentId,
            Project project,
            WorkerLookupCache lookupCache) throws Exception {

        List<XmlTableMapping> inlines = inlinesByParentId.get(parentMapping.getId());
        // Batch-fetch all inline children for every row in `rows` at once
        Map<String, Map<String, List<Map<String, Object>>>> inlineData =
                batchFetchXmlInlines(jdbcConn, inlines, rows, parentMapping, project, lookupCache);

        List<XmlDocumentBuilder.MappedRow> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> inlineChildData = new LinkedHashMap<>();
            if (inlines != null) {
                for (XmlTableMapping inline : inlines) {
                    try {
                        JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);
                        Object joinVal = row.get(joinPath.parentColumn());
                        if (joinVal == null) { inlineChildData.put(inline, List.of()); continue; }
                        String key = String.valueOf(joinVal);
                        List<Map<String, Object>> inlineRows;
                        if (lookupCache.isCached(inline.getId())) {
                            inlineRows = lookupCache.lookup(inline.getId(), joinPath.childColumn(), key);
                        } else {
                            inlineRows = inlineData.getOrDefault(inline.getId(), Map.of()).getOrDefault(key, List.of());
                        }
                        // Only one level of inline batch-fetching; deeper nesting falls back to per-row queries
                        List<XmlDocumentBuilder.MappedRow> inlineMapped = new ArrayList<>(inlineRows.size());
                        for (Map<String, Object> ir : inlineRows) {
                            inlineMapped.add(new XmlDocumentBuilder.MappedRow(ir, Map.of()));
                        }
                        inlineChildData.put(inline, inlineMapped);
                    } catch (Exception e) {
                        inlineChildData.put(inline, List.of());
                    }
                }
            }
            result.add(new XmlDocumentBuilder.MappedRow(row, inlineChildData));
        }
        return result;
    }

    private List<JsonDocumentBuilder.MappedRow> toJsonMappedRows(
            java.sql.Connection jdbcConn,
            JsonTableMapping parentMapping,
            List<Map<String, Object>> rows,
            Map<String, List<JsonTableMapping>> inlinesByParentId,
            Project project,
            WorkerLookupCache lookupCache) throws Exception {

        List<JsonTableMapping> inlines = inlinesByParentId.get(parentMapping.getId());
        Map<String, Map<String, List<Map<String, Object>>>> inlineData =
                batchFetchJsonInlines(jdbcConn, inlines, rows, parentMapping, project, lookupCache);

        List<JsonDocumentBuilder.MappedRow> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> inlineChildData = new LinkedHashMap<>();
            if (inlines != null) {
                for (JsonTableMapping inline : inlines) {
                    try {
                        JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);
                        Object joinVal = row.get(joinPath.parentColumn());
                        if (joinVal == null) { inlineChildData.put(inline, List.of()); continue; }
                        String key = String.valueOf(joinVal);
                        List<Map<String, Object>> inlineRows;
                        if (lookupCache.isCached(inline.getId())) {
                            inlineRows = lookupCache.lookup(inline.getId(), joinPath.childColumn(), key);
                        } else {
                            inlineRows = inlineData.getOrDefault(inline.getId(), Map.of()).getOrDefault(key, List.of());
                        }
                        List<JsonDocumentBuilder.MappedRow> inlineMapped = new ArrayList<>(inlineRows.size());
                        for (Map<String, Object> ir : inlineRows) {
                            inlineMapped.add(new JsonDocumentBuilder.MappedRow(ir, Map.of()));
                        }
                        inlineChildData.put(inline, inlineMapped);
                    } catch (Exception e) {
                        inlineChildData.put(inline, List.of());
                    }
                }
            }
            result.add(new JsonDocumentBuilder.MappedRow(row, inlineChildData));
        }
        return result;
    }

    // ── Level-2 inline batch fetch ────────────────────────────────────────────

    /**
     * Fetches all inline (2nd-level) child rows for every parent row in {@code parentRows}
     * using one IN query per inline mapping — eliminates the previous per-row N+1 pattern.
     */
    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchXmlInlines(
            java.sql.Connection jdbcConn,
            List<XmlTableMapping> inlines,
            List<Map<String, Object>> parentRows,
            XmlTableMapping parentMapping,
            Project project,
            WorkerLookupCache lookupCache) {

        if (inlines == null || inlines.isEmpty() || parentRows.isEmpty()) return Map.of();
        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (XmlTableMapping inline : inlines) {
            if (lookupCache.isCached(inline.getId())) continue;
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);
                List<Object> fkValues = parentRows.stream()
                        .map(r -> r.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (fkValues.isEmpty()) continue;
                String sql = sqlQueryBuilder.buildChildBatchQuery(inline, joinPath.childColumn(), fkValues.size());
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < fkValues.size(); i++) stmt.setObject(i + 1, fkValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs, cols);
                            String key = String.valueOf(row.get(joinPath.childColumn()));
                            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                result.put(inline.getId(), grouped);
            } catch (Exception e) {
                log.warn("Inline batch fetch failed for '{}': {}", inline.getXmlName(), e.getMessage());
                result.put(inline.getId(), Map.of());
            }
        }
        return result;
    }

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchJsonInlines(
            java.sql.Connection jdbcConn,
            List<JsonTableMapping> inlines,
            List<Map<String, Object>> parentRows,
            JsonTableMapping parentMapping,
            Project project,
            WorkerLookupCache lookupCache) {

        if (inlines == null || inlines.isEmpty() || parentRows.isEmpty()) return Map.of();
        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (JsonTableMapping inline : inlines) {
            if (lookupCache.isCached(inline.getId())) continue;
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);
                List<Object> fkValues = parentRows.stream()
                        .map(r -> r.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (fkValues.isEmpty()) continue;
                String sql = sqlQueryBuilder.buildChildBatchQuery(inline, joinPath.childColumn(), fkValues.size());
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < fkValues.size(); i++) stmt.setObject(i + 1, fkValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs, cols);
                            String key = String.valueOf(row.get(joinPath.childColumn()));
                            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                result.put(inline.getId(), grouped);
            } catch (Exception e) {
                log.warn("Inline batch fetch failed for '{}': {}", inline.getJsonName(), e.getMessage());
                result.put(inline.getId(), Map.of());
            }
        }
        return result;
    }

    // ── SQL helpers ───────────────────────────────────────────────────────────

    private String buildFullCursorSql(java.sql.Connection jdbcConn, String mappingType,
                                       Project project, Connection.ConnectionType dbType) {
        String whereClause = null;
        String schema, table, cols;

        if ("JSON".equalsIgnoreCase(mappingType)) {
            JsonTableMapping root = project.getMapping().getJsonDocumentModel().getRoot();
            schema = root.getSourceSchema();
            table  = root.getSourceTable();
            whereClause = lookupWhereClause(schema, table, project);
            cols = buildJsonColList(root);
        } else {
            XmlTableMapping root = project.getMapping().getDocumentModel().getRoot();
            schema = root.getSourceSchema();
            table  = root.getSourceTable();
            whereClause = lookupWhereClause(schema, table, project);
            cols = buildXmlColList(root);
        }

        String qualifiedTable = qualifiedTable(schema, table);
        String sql = "SELECT " + cols + " FROM " + qualifiedTable;
        if (whereClause != null && !whereClause.isBlank()) sql += " WHERE " + whereClause;
        return sql;
    }

    private String buildXmlColList(XmlTableMapping mapping) {
        if (mapping.getColumns() == null || mapping.getColumns().isEmpty()) return "*";
        return mapping.getColumns().stream()
                .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                .map(c -> quote(c.getSourceColumn()))
                .reduce((a, b) -> a + ", " + b).orElse("*");
    }

    private String buildJsonColList(JsonTableMapping mapping) {
        if (mapping.getColumns() == null || mapping.getColumns().isEmpty()) return "*";
        return mapping.getColumns().stream()
                .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                .map(c -> quote(c.getSourceColumn()))
                .reduce((a, b) -> a + ", " + b).orElse("*");
    }

    private String lookupWhereClause(String schema, String table, Project project) {
        if (project.getSchemas() == null) return null;
        var dbSchema = project.getSchemas().get(schema);
        if (dbSchema == null || dbSchema.getTables() == null) return null;
        var dbTable = dbSchema.getTables().get(table);
        return dbTable != null ? dbTable.getWhereClause() : null;
    }

    private String qualifiedTable(String schema, String table) {
        if (schema != null && !schema.isBlank()) {
            String quotedSchema = Arrays.stream(schema.split("\\."))
                    .map(this::quote)
                    .collect(Collectors.joining("."));
            return quotedSchema + "." + quote(table);
        }
        return quote(table);
    }

    private String quote(String id) {
        return "\"" + id.replace("\"", "\"\"") + "\"";
    }

    /**
     * Extracts column names from a ResultSet once and returns them as a reusable array.
     * Pass the returned array to {@link #toMap(ResultSet, String[])} for every subsequent row
     * to avoid redundant {@code getMetaData()} calls on the hot path.
     */
    private static String[] columnNames(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int n = meta.getColumnCount();
        String[] names = new String[n];
        for (int i = 0; i < n; i++) names[i] = meta.getColumnName(i + 1);
        return names;
    }

    private static Map<String, Object> toMap(ResultSet rs, String[] cols) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>(cols.length * 2);
        for (int i = 0; i < cols.length; i++) row.put(cols[i], rs.getObject(i + 1));
        return row;
    }


    // ── URI building ──────────────────────────────────────────────────────────

    private String buildUri(Map<String, Object> rootRow, int workerIndex, int batchNumber, int idxInBatch,
                             String mappingType, WorkerMappingState state) {
        String dirPath = ctx.getDirectoryPath();
        if (dirPath == null || dirPath.isBlank()) dirPath = "/";
        if (!dirPath.endsWith("/")) dirPath += "/";

        String rootElement = "JSON".equalsIgnoreCase(mappingType)
                ? (state.jsonRootMapping != null ? state.jsonRootMapping.getJsonName() : "doc")
                : (state.xmlRootMapping  != null ? state.xmlRootMapping.getXmlName()   : "doc");

        // Global index: workerIndex * large_prime + batchNumber * BATCH_SIZE + idxInBatch would
        // not be truly sequential, so we prefer PK-based URIs. Fall back to a composite index.
        int globalIdx = batchNumber * BATCH_SIZE + idxInBatch;
        String dir = dirPath.replace("{rootElement}", rootElement)
                             .replace("{index}", String.valueOf(globalIdx));
        String pkValue = findPkValue(rootRow);
        return dir + (pkValue != null ? pkValue : (workerIndex + "_" + batchNumber + "_" + idxInBatch));
    }

    private String findPkValue(Map<String, Object> row) {
        for (String key : row.keySet()) {
            String lower = key.toLowerCase();
            if (lower.equals("id") || lower.endsWith("_id") || lower.endsWith("id")) {
                Object val = row.get(key);
                if (val != null) return val.toString();
            }
        }
        return null;
    }

    // ── Mapping type resolution ───────────────────────────────────────────────

    private String resolveMappingType() {
        Project project = ctx.getProject();
        String mt = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";
        return mt != null ? mt : "XML";
    }

    // ── MarkLogic client ──────────────────────────────────────────────────────

    private DatabaseClient buildMarkLogicClient() {
        MarkLogicConnection mlConn = ctx.getMarklogicConnection().getConnection();
        String plainPassword = passwordEncryptionService.decrypt(mlConn.getPassword());
        String host     = mlConn.getHost();
        int    port     = mlConn.getPort() != null ? mlConn.getPort() : 8000;
        String database = (mlConn.getDatabase() != null && !mlConn.getDatabase().isBlank())
                ? mlConn.getDatabase() : null;
        boolean useSSL  = Boolean.TRUE.equals(mlConn.getUseSSL());

        DatabaseClientFactory.SecurityContext secCtx;
        if ("basic".equalsIgnoreCase(mlConn.getAuthType())) {
            var ctx = new DatabaseClientFactory.BasicAuthContext(mlConn.getUsername(), plainPassword);
            if (useSSL) ctx.withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
            secCtx = ctx;
        } else {
            var ctx = new DatabaseClientFactory.DigestAuthContext(mlConn.getUsername(), plainPassword);
            if (useSSL) ctx.withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
            secCtx = ctx;
        }
        return database != null
                ? DatabaseClientFactory.newClient(host, port, database, secCtx)
                : DatabaseClientFactory.newClient(host, port, secCtx);
    }

    // ── Inner helpers ─────────────────────────────────────────────────────────

    /**
     * Per-worker mapping state: resolved once at worker startup, then read-only.
     * All fields are immutable after construction so concurrent workers sharing
     * project metadata are safe.
     */
    private static class WorkerMappingState {
        final String mappingType;
        final XmlTableMapping xmlRootMapping;
        final List<XmlTableMapping> xmlRootLevelMappings;
        final Map<String, List<XmlTableMapping>> xmlInlinesByParentId;
        final JsonTableMapping jsonRootMapping;
        final List<JsonTableMapping> jsonRootLevelMappings;
        final Map<String, List<JsonTableMapping>> jsonInlinesByParentId;

        WorkerMappingState(String mappingType, Project project) {
            this.mappingType = mappingType;
            if ("JSON".equalsIgnoreCase(mappingType)) {
                JsonDocumentModel dm = project.getMapping().getJsonDocumentModel();
                jsonRootMapping = dm.getRoot();
                List<JsonTableMapping> all = dm.getElements() != null ? dm.getElements() : List.of();
                Map<String, List<JsonTableMapping>> inlines = new LinkedHashMap<>();
                for (JsonTableMapping m : all) {
                    if (m.getParentRef() != null
                            && ("InlineObject".equals(m.getMappingType()) || "Array".equals(m.getMappingType()))) {
                        inlines.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
                    }
                }
                jsonInlinesByParentId  = Collections.unmodifiableMap(inlines);
                jsonRootLevelMappings  = all.stream().filter(m -> m.getParentRef() == null).toList();
                xmlRootMapping         = null;
                xmlRootLevelMappings   = List.of();
                xmlInlinesByParentId   = Map.of();
            } else {
                DocumentModel dm = project.getMapping().getDocumentModel();
                xmlRootMapping = dm.getRoot();
                List<XmlTableMapping> all = dm.getElements() != null ? dm.getElements() : List.of();
                Map<String, List<XmlTableMapping>> inlines = new LinkedHashMap<>();
                for (XmlTableMapping m : all) {
                    if (m.getParentRef() != null) {
                        inlines.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
                    }
                }
                xmlInlinesByParentId  = Collections.unmodifiableMap(inlines);
                xmlRootLevelMappings  = all.stream().filter(m -> m.getParentRef() == null).toList();
                jsonRootMapping       = null;
                jsonRootLevelMappings = List.of();
                jsonInlinesByParentId = Map.of();
            }
        }
    }

    /**
     * Per-worker in-memory lookup cache for small child/reference tables.
     * Loaded once during worker startup; all subsequent lookups are O(1) HashMap gets.
     * Each worker has its own instance — no sharing, no synchronization needed.
     */
    private class WorkerLookupCache {
        private final Map<String, List<Map<String, Object>>> rawRows   = new HashMap<>();
        private final Map<String, Map<String, List<Map<String, Object>>>> indices = new HashMap<>();
        private final Set<String> cachedIds = new HashSet<>();

        WorkerLookupCache(java.sql.Connection jdbcConn, String mappingType, Project project) {
            Project p = ctx.getProject();
            if ("JSON".equalsIgnoreCase(mappingType)) {
                List<JsonTableMapping> all = p.getMapping().getJsonDocumentModel().getElements();
                if (all != null) all.forEach(m -> tryCache(jdbcConn, m.getId(), m.getSourceSchema(), m.getSourceTable()));
            } else {
                List<XmlTableMapping> all = p.getMapping().getDocumentModel().getElements();
                if (all != null) all.forEach(m -> tryCache(jdbcConn, m.getId(), m.getSourceSchema(), m.getSourceTable()));
            }
        }

        private void tryCache(java.sql.Connection jdbcConn, String mappingId, String schema, String table) {
            try {
                long count;
                String countSql = "SELECT COUNT(*) FROM " + qualifiedTable(schema, table);
                try (PreparedStatement s = jdbcConn.prepareStatement(countSql);
                     ResultSet rs = s.executeQuery()) {
                    count = rs.next() ? rs.getLong(1) : 0;
                }
                if (count > 0 && count <= LOOKUP_CACHE_THRESHOLD) {
                    long t0 = System.nanoTime();
                    String dataSql = "SELECT * FROM " + qualifiedTable(schema, table);
                    List<Map<String, Object>> rows = new ArrayList<>();
                    try (PreparedStatement s = jdbcConn.prepareStatement(dataSql);
                         ResultSet rs = s.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) rows.add(toMap(rs, cols));
                    }
                    metrics.recordNanos(metrics.dbLookupCacheLoadTimer, System.nanoTime() - t0);
                    rawRows.put(mappingId, rows);
                    cachedIds.add(mappingId);
                    log.debug("Worker cached '{}' ({} rows)", table, rows.size());
                }
            } catch (Exception e) {
                log.debug("Skipping cache for '{}': {}", table, e.getMessage());
            }
        }

        boolean isCached(String mappingId) { return cachedIds.contains(mappingId); }

        List<Map<String, Object>> lookup(String mappingId, String colName, String colValue) {
            String indexKey = mappingId + ":" + colName;
            if (!indices.containsKey(indexKey)) {
                List<Map<String, Object>> all = rawRows.getOrDefault(mappingId, List.of());
                Map<String, List<Map<String, Object>>> idx = new HashMap<>();
                for (Map<String, Object> row : all) {
                    Object v = row.get(colName);
                    if (v != null) idx.computeIfAbsent(String.valueOf(v), k -> new ArrayList<>()).add(row);
                }
                indices.put(indexKey, idx);
            }
            return indices.get(indexKey).getOrDefault(colValue, List.of());
        }
    }
}
