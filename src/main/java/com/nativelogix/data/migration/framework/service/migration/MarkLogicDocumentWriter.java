package com.nativelogix.data.migration.framework.service.migration;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.service.MarkLogicSecurityService;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;

import java.util.List;


/**
 * Spring Batch ItemWriter that writes document chunks to MarkLogic using the
 * Data Movement SDK (DMSDK) {@link WriteBatcher}.
 *
 * <h3>Performance optimisations</h3>
 * <ul>
 *   <li><b>DMSDK WriteBatcher</b> — documents are dispatched to MarkLogic on
 *       {@value #BATCHER_THREAD_COUNT} parallel HTTP writer threads, each sending
 *       batches of {@value #BATCHER_BATCH_SIZE} documents.  This pipelines JDBC
 *       reading and ML writing so they overlap in time.</li>
 *   <li><b>Persistent DatabaseClient</b> — opened once in {@link #open}, shared
 *       across all chunks, eliminating TLS/auth overhead per chunk.</li>
 *   <li><b>Metadata built once</b> — the {@link DocumentMetadataHandle} is created
 *       in {@link #open} and reused for every document; no per-chunk allocation.</li>
 *   <li><b>Auth warm-up</b> — a lightweight {@code exists()} call pre-authenticates
 *       the connection so the first real write does not pay the 401 round-trip.</li>
 * </ul>
 */
public class MarkLogicDocumentWriter implements ItemWriter<DocumentBuildResult>, ItemStream {

    private static final Logger log = LoggerFactory.getLogger(  MarkLogicDocumentWriter.class);

    private final MigrationJobContext ctx;
    private final PasswordEncryptionService encryptionService;
    private final MarkLogicSecurityService securityService;
    private final MigrationMetrics metrics;
    /** Documents per HTTP request sent by each WriteBatcher worker thread. */
    private final int batcherBatchSize;
    /** Parallel HTTP writer threads within the WriteBatcher. */
    private final int batcherThreadCount;

    /** Shared across all chunks; opened in {@link #open}, released in {@link #close}. */
    private DatabaseClient sharedClient;
    private DataMovementManager dmm;
    private WriteBatcher batcher;

    /**
     * Fallback document manager used when the DMSDK WriteBatcher cannot be initialised
     * (e.g. the connected user lacks the {@code rest-reader} role needed by
     * {@code GET /v1/internal/forestinfo}).  Writes are synchronous and single-threaded
     * in this mode but functionally correct.
     */
    private GenericDocumentManager directDocManager;

    /** True when DMSDK init failed and we fell back to direct DocumentManager writes. */
    private boolean directWrite = false;

    /** Built once in {@link #open} from securityConfig; reused per document. Null means no metadata (ML defaults apply). */
    private DocumentMetadataHandle metadata;

    /** Server-side REST transform to apply on ingest. Null means no transform. */
    private ServerTransform serverTransform;

    /** Captures async batcher failures; checked at the start of each {@link #write} call. */
    private volatile Exception writeError;

    public MarkLogicDocumentWriter(MigrationJobContext ctx, PasswordEncryptionService encryptionService,
                                   MarkLogicSecurityService securityService,
                                   MigrationMetrics metrics,
                                   int batcherBatchSize, int batcherThreadCount) {
        this.ctx                = ctx;
        this.encryptionService  = encryptionService;
        this.securityService    = securityService;
        this.metrics            = metrics;
        this.batcherBatchSize   = batcherBatchSize;
        this.batcherThreadCount = batcherThreadCount;
    }

    // ── ItemStream ────────────────────────────────────────────────────────────

    @Override
    public void open(@org.springframework.lang.NonNull ExecutionContext executionContext) {
        MarkLogicConnection mlConn = ctx.getMarklogicConnection().getConnection();
        String plainPassword = encryptionService.decrypt(mlConn.getPassword());
        sharedClient = buildClient(mlConn, plainPassword);

        // Pre-authenticate: complete the Digest handshake before the first real write.
        try {
            sharedClient.newDocumentManager().exists("/__rdbms2ml_warmup__");
        } catch (Exception ignored) {}

        // Build the metadata handle once from the effective security config (permissions, collections, quality, metadata).
        metadata = securityService.buildMetadataHandle(ctx.getSecurityConfig());

        // Build the ServerTransform if a transform name was specified.
        if (ctx.getTransformName() != null && !ctx.getTransformName().isBlank()) {
            serverTransform = new ServerTransform(ctx.getTransformName());
            if (ctx.getTransformParams() != null) {
                ctx.getTransformParams().forEach(serverTransform::addParameter);
            }
            log.debug("MarkLogic ingest transform configured: {}", ctx.getTransformName());
        }

        // Attempt DMSDK WriteBatcher setup. If the connected user lacks rest-reader/rest-writer
        // (needed for GET /v1/internal/forestinfo), fall back to direct DocumentManager writes.
        try {
            dmm = sharedClient.newDataMovementManager();
            WriteBatcher wb = dmm.newWriteBatcher()
                    .withBatchSize(batcherBatchSize)
                    .withThreadCount(batcherThreadCount);
            if (serverTransform != null) {
                wb.withTransform(serverTransform);
            }
            batcher = wb
                    .onBatchSuccess(batch -> {
                        int n = batch.getItems().length;
                        metrics.mlDocsWrittenCounter.increment(n);
                        log.debug("WriteBatcher wrote batch of {} docs", n);
                    })
                    .onBatchFailure((batch, throwable) -> {
                        metrics.mlWriteErrorCounter.increment(batch.getItems().length);
                        log.error("WriteBatcher batch failed: {}", throwable.getMessage(), throwable);
                        writeError = new RuntimeException(
                                "MarkLogic write failed: " + throwable.getMessage(), throwable);
                    });
            dmm.startJob(batcher);
            log.debug("MarkLogic WriteBatcher opened for {} (batchSize={}, threads={})",
                    mlConn.getHost(), batcherBatchSize, batcherThreadCount);
        } catch (Exception e) {
            log.warn("DMSDK WriteBatcher unavailable ({}); falling back to direct DocumentManager writes. " +
                     "Grant the connected user rest-reader/rest-writer to enable parallel batching.",
                     e.getMessage());
            directWrite = true;
            directDocManager = sharedClient.newDocumentManager();
            batcher = null;
            dmm     = null;
        }
    }

    @Override
    public void update(@org.springframework.lang.NonNull ExecutionContext executionContext) {
        // no checkpoint state to persist
    }

    @Override
    public void close() {
        try {
            if (batcher != null) {
                batcher.flushAndWait();  // Drain any buffered docs before releasing the client.
                dmm.stopJob(batcher);
            }
        } catch (Exception e) {
            log.warn("Error flushing WriteBatcher on close: {}", e.getMessage());
        }
        try {
            if (sharedClient != null) {
                sharedClient.release();
                log.debug("MarkLogic DatabaseClient released");
            }
        } catch (Exception ignored) {}
        sharedClient = null;
        batcher      = null;
        dmm          = null;
    }

    // ── ItemWriter ────────────────────────────────────────────────────────────

    @Override
    public void write(@org.springframework.lang.NonNull Chunk<? extends DocumentBuildResult> chunk) throws Exception {
        // Propagate any async write failure captured by the batcher's onBatchFailure callback.
        if (writeError != null) throw writeError;

        List<? extends DocumentBuildResult> items = chunk.getItems();
        if (items.isEmpty()) return;

        Format format = "JSON".equalsIgnoreCase(items.get(0).getFormat()) ? Format.JSON : Format.XML;

        long t0 = System.nanoTime();
        try {
            if (directWrite) {
                // Fallback path: synchronous writes via DocumentWriteSet.
                // DocumentMetadataHandle implements GenericWriteHandle so write(uri, metadata, content)
                // resolves to the wrong overload — DocumentWriteSet.add() is correctly typed.
                DocumentWriteSet writeSet = directDocManager.newWriteSet();
                for (DocumentBuildResult doc : items) {
                    StringHandle content = new StringHandle(doc.getContent()).withFormat(format);
                    if (metadata != null) {
                        writeSet.add(doc.getUri(), metadata, content);
                    } else {
                        writeSet.add(doc.getUri(), content);
                    }
                }
                if (serverTransform != null) {
                    directDocManager.write(writeSet, serverTransform);
                } else {
                    directDocManager.write(writeSet);
                }
                metrics.mlDocsWrittenCounter.increment(items.size());
                log.debug("Direct-wrote {} documents to MarkLogic", items.size());
            } else {
                // Normal path: queue into DMSDK WriteBatcher for parallel async writes.
                for (DocumentBuildResult doc : items) {
                    StringHandle content = new StringHandle(doc.getContent()).withFormat(format);
                    if (metadata != null) {
                        batcher.add(doc.getUri(), metadata, content);
                    } else {
                        batcher.add(doc.getUri(), content);
                    }
                }
                // The batcher auto-flushes when BATCHER_BATCH_SIZE is reached on its worker threads.
                // Remaining buffered docs are flushed in close() via flushAndWait().
                log.debug("Queued {} documents to WriteBatcher", items.size());
            }
        } finally {
            metrics.recordNanos(metrics.mlWriteTimer, System.nanoTime() - t0);
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private DatabaseClient buildClient(MarkLogicConnection connection, String plainPassword) {
        String host     = connection.getHost();
        int    port     = connection.getPort() != null ? connection.getPort() : 8000;
        String database = (connection.getDatabase() != null && !connection.getDatabase().isBlank())
                ? connection.getDatabase() : null;
        boolean useSSL  = Boolean.TRUE.equals(connection.getUseSSL());

        DatabaseClientFactory.SecurityContext securityContext;
        if ("basic".equalsIgnoreCase(connection.getAuthType())) {
            var ctx = new DatabaseClientFactory.BasicAuthContext(connection.getUsername(), plainPassword);
            if (useSSL) ctx.withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
            securityContext = ctx;
        } else {
            var ctx = new DatabaseClientFactory.DigestAuthContext(connection.getUsername(), plainPassword);
            if (useSSL) ctx.withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
            securityContext = ctx;
        }

        return database != null
                ? DatabaseClientFactory.newClient(host, port, database, securityContext)
                : DatabaseClientFactory.newClient(host, port, securityContext);
    }
}
