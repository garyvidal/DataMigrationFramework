package com.nativelogix.rdbms2marklogic.service;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.nativelogix.rdbms2marklogic.model.ConnectionTestResult;
import com.nativelogix.rdbms2marklogic.model.MarkLogicConnection;
import com.nativelogix.rdbms2marklogic.model.SavedMarkLogicConnection;
import com.nativelogix.rdbms2marklogic.repository.MarkLogicConnectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MarkLogicConnectionService {

    private final MarkLogicConnectionRepository repository;
    private final PasswordEncryptionService encryptionService;

    public SavedMarkLogicConnection saveConnection(SavedMarkLogicConnection sc) {
        if (sc.getName() == null || sc.getName().isBlank()) {
            throw new IllegalArgumentException("Connection name cannot be empty");
        }
        return repository.save(sc);
    }

    public SavedMarkLogicConnection updateConnection(String originalName, SavedMarkLogicConnection updated) {
        if (updated.getName() == null || updated.getName().isBlank()) {
            throw new IllegalArgumentException("Connection name cannot be empty");
        }
        return repository.update(originalName, updated);
    }

    public Optional<SavedMarkLogicConnection> getConnection(String name) {
        return repository.findByName(name);
    }

    public List<SavedMarkLogicConnection> getAllConnections() {
        return repository.findAll();
    }

    public void deleteConnection(String name) {
        repository.delete(name);
    }

    public boolean connectionExists(String name) {
        return repository.exists(name);
    }

    /**
     * Tests a saved connection using its stored (decrypted) credentials, looked up by id.
     */
    public ConnectionTestResult testConnectionById(String id) {
        SavedMarkLogicConnection sc = getAllConnections().stream()
                .filter(c -> id.equals(c.getId()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("MarkLogic connection not found: " + id));
        return testConnection(sc.getConnection());
    }

    /**
     * Tests a MarkLogic connection by attempting to open a DatabaseClient and
     * performing a lightweight read operation. The password in the supplied
     * {@link MarkLogicConnection} must be plaintext (already decrypted by the caller).
     */
    public ConnectionTestResult testConnection(MarkLogicConnection connection) {
        DatabaseClient client = null;
        try {
            String plainPassword = encryptionService.decrypt(connection.getPassword());
            client = buildClient(connection, plainPassword);
            // Perform a lightweight read to verify the server actually accepts the credentials
            client.newDocumentManager().exists("/__connection-test__");
            return new ConnectionTestResult(true, "Connection successful");
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : "Connection failed";
            return new ConnectionTestResult(false, message);
        } finally {
            if (client != null) {
                try { client.release(); } catch (Exception ignored) {}
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private DatabaseClient buildClient(MarkLogicConnection connection, String plainPassword) {
        String host = connection.getHost();
        int port = connection.getPort() != null ? connection.getPort() : 8000;
        String database = (connection.getDatabase() != null && !connection.getDatabase().isBlank())
                ? connection.getDatabase() : null;
        boolean useSSL = Boolean.TRUE.equals(connection.getUseSSL());

        DatabaseClientFactory.SecurityContext securityContext = buildSecurityContext(
                connection.getUsername(), plainPassword, connection.getAuthType(), useSSL);

        if (database != null) {
            return DatabaseClientFactory.newClient(host, port, database, securityContext);
        }
        return DatabaseClientFactory.newClient(host, port, securityContext);
    }

    private DatabaseClientFactory.SecurityContext buildSecurityContext(
            String username, String password, String authType, boolean useSSL) {

        DatabaseClientFactory.SecurityContext ctx;
        if ("basic".equalsIgnoreCase(authType)) {
            ctx = new DatabaseClientFactory.BasicAuthContext(username, password);
        } else {
            ctx = new DatabaseClientFactory.DigestAuthContext(username, password);
        }

        return ctx;
    }
}
