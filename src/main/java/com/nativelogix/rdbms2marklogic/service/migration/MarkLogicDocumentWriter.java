package com.nativelogix.rdbms2marklogic.service.migration;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.nativelogix.rdbms2marklogic.model.MarkLogicConnection;
import com.nativelogix.rdbms2marklogic.service.PasswordEncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

/**
 * Spring Batch ItemWriter that writes document chunks to MarkLogic using the Java Client API.
 * <p>
 * Documents are written one at a time (PUT /v1/documents) rather than via multipart bulk POST.
 * This avoids the OkHttp Digest-auth + multipart body replay limitation, where the 401
 * challenge-response flow fails because multipart streams cannot be rewound.
 * <p>
 * A single DatabaseClient is opened per chunk and released after all writes in the chunk complete.
 */
public class MarkLogicDocumentWriter implements ItemWriter<DocumentBuildResult> {

    private static final Logger log = LoggerFactory.getLogger(MarkLogicDocumentWriter.class);

    private final MigrationJobContext ctx;
    private final PasswordEncryptionService encryptionService;

    public MarkLogicDocumentWriter(MigrationJobContext ctx, PasswordEncryptionService encryptionService) {
        this.ctx = ctx;
        this.encryptionService = encryptionService;
    }

    @Override
    public void write(@org.springframework.lang.NonNull Chunk<? extends DocumentBuildResult> chunk) throws Exception {
        List<? extends DocumentBuildResult> items = chunk.getItems();
        if (items.isEmpty()) return;

        MarkLogicConnection mlConn = ctx.getMarklogicConnection().getConnection();
        String plainPassword = encryptionService.decrypt(mlConn.getPassword());
        DatabaseClient client = buildClient(mlConn, plainPassword);
        try {
            boolean isJson = "JSON".equalsIgnoreCase(items.get(0).getFormat());

            // Write content and metadata as separate single-part PUTs.
            // Combining them (3-arg write) sends a multipart body, which OkHttp cannot
            // replay after a Digest auth 401 challenge — causing ResourceNotFoundException.
            boolean hasCollections = ctx.getCollections() != null && !ctx.getCollections().isEmpty();

            if (isJson) {
                JSONDocumentManager mgr = client.newJSONDocumentManager();
                for (DocumentBuildResult doc : items) {
                    mgr.write(doc.getUri(), new StringHandle(doc.getContent()).withFormat(Format.JSON));
                    if (hasCollections) mgr.writeMetadata(doc.getUri(), buildMetadata());
                }
            } else {
                XMLDocumentManager mgr = client.newXMLDocumentManager();
                for (DocumentBuildResult doc : items) {
                    mgr.write(doc.getUri(), new StringHandle(doc.getContent()).withFormat(Format.XML));
                    if (hasCollections) mgr.writeMetadata(doc.getUri(), buildMetadata());
                }
            }

            log.debug("Wrote {} documents to MarkLogic", items.size());
        } finally {
            try { client.release(); } catch (Exception ignored) {}
        }
    }

    private DocumentMetadataHandle buildMetadata() {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (ctx.getCollections() != null && !ctx.getCollections().isEmpty()) {
            metadata.getCollections().addAll(ctx.getCollections());
        }
        return metadata;
    }

    private DatabaseClient buildClient(MarkLogicConnection connection, String plainPassword) {
        String host = connection.getHost();
        int port = connection.getPort() != null ? connection.getPort() : 8000;
        String database = (connection.getDatabase() != null && !connection.getDatabase().isBlank())
                ? connection.getDatabase() : null;
        boolean useSSL = Boolean.TRUE.equals(connection.getUseSSL());

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

        if (database != null) {
            return DatabaseClientFactory.newClient(host, port, database, securityContext);
        }
        return DatabaseClientFactory.newClient(host, port, securityContext);
    }
}
