package com.nativelogix.rdbms2marklogic.service.migration;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.nativelogix.rdbms2marklogic.model.MarkLogicConnection;
import com.nativelogix.rdbms2marklogic.service.PasswordEncryptionService;
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

    private static final Logger log = LoggerFactory.getLogger(MarkLogicDocumentWriter.class);

    private final MigrationJobContext ctx;
    private final PasswordEncryptionService encryptionService;
    /** Documents per HTTP request sent by each WriteBatcher worker thread. */
    private final int batcherBatchSize;
    /** Parallel HTTP writer threads within the WriteBatcher. */
    private final int batcherThreadCount;

    /** Shared across all chunks; opened in {@link #open}, released in {@link #close}. */
    private DatabaseClient sharedClient;
    private DataMovementManager dmm;
    private WriteBatcher batcher;

    /** Built once in {@link #open} if collections are configured; reused per document. */
    private DocumentMetadataHandle metadata;

    /** Captures async batcher failures; checked at the start of each {@link #write} call. */
    private volatile Exception writeError;

    public MarkLogicDocumentWriter(MigrationJobContext ctx, PasswordEncryptionService encryptionService,
                                   int batcherBatchSize, int batcherThreadCount) {
        this.ctx                = ctx;
        this.encryptionService  = encryptionService;
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

        // Build the metadata handle once if collections are configured.
        boolean hasCollections = ctx.getCollections() != null && !ctx.getCollections().isEmpty();
        if (hasCollections) {
            metadata = new DocumentMetadataHandle();
            metadata.getCollections().addAll(ctx.getCollections());
        }

        // Set up DMSDK WriteBatcher for parallel async writes.
        dmm = sharedClient.newDataMovementManager();
        batcher = dmm.newWriteBatcher()
                .withBatchSize(batcherBatchSize)
                .withThreadCount(batcherThreadCount)
                .onBatchSuccess(batch ->
                        log.debug("WriteBatcher wrote batch of {} docs", batch.getItems().length))
                .onBatchFailure((batch, throwable) -> {
                    log.error("WriteBatcher batch failed: {}", throwable.getMessage(), throwable);
                    writeError = new RuntimeException(
                            "MarkLogic write failed: " + throwable.getMessage(), throwable);
                });
        dmm.startJob(batcher);

        log.debug("MarkLogic WriteBatcher opened for {} (batchSize={}, threads={})",
                mlConn.getHost(), batcherBatchSize, batcherThreadCount);
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
