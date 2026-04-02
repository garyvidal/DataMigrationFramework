package com.nativelogix.data.migration.framework.service;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.ConnectionTestResult;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import us.fatehi.utility.datasource.DatabaseConnectionSourceBuilder;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class JDBCConnectionService {

    private final ConnectionRepository connectionRepository;
    private final PasswordEncryptionService encryptionService;

    public SavedConnection saveConnection(SavedConnection savedConnection) {
        if (savedConnection.getName() == null || savedConnection.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Connection name cannot be empty");
        }
        return connectionRepository.save(savedConnection);
    }

    public SavedConnection updateConnection(String originalName, SavedConnection updated) {
        if (updated.getName() == null || updated.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Connection name cannot be empty");
        }
        return connectionRepository.update(originalName, updated);
    }

    public Optional<SavedConnection> getConnection(String name) {
        return connectionRepository.findByName(name);
    }

    public List<SavedConnection> getAllConnections() {
        return connectionRepository.findAll();
    }

    public void deleteConnection(String name) {
        connectionRepository.delete(name);
    }

    public boolean connectionExists(String name) {
        return connectionRepository.exists(name);
    }

    /** Tests a connection using its stored (decrypted) credentials, looked up by id. */
    public ConnectionTestResult testConnectionById(String id) {
        SavedConnection sc = getAllConnections().stream()
                .filter(c -> id.equals(c.getId()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Connection not found: " + id));
        return testConnection(sc.getConnection());
    }

    public ConnectionTestResult testConnection(Connection connection) {
        try (java.sql.Connection conn = openJdbcConnection(connection)) {
            return new ConnectionTestResult(true, "Connection successful");
        } catch (Exception e) {
            return new ConnectionTestResult(false, e.getMessage() != null ? e.getMessage() : "Connection failed");
        }
    }

    /**
     * Opens and returns a live {@link java.sql.Connection} for the given connection config.
     * Callers are responsible for closing it (use try-with-resources).
     */
    public java.sql.Connection openJdbcConnection(Connection connection) throws Exception {
        String jdbcUrl = buildJdbcUrl(connection);
        String password = encryptionService.decrypt(connection.getPassword());
        var dataSource = DatabaseConnectionSourceBuilder
                .builder(jdbcUrl)
                .withUserCredentials(new MultiUseUserCredentials(connection.getUserName(), password))
                .build();
        return dataSource.get();
    }

    private String buildJdbcUrl(Connection connection) {
        // If the user entered a JDBC URI directly, use it as-is
        if (Boolean.TRUE.equals(connection.getEnterUriManually())
                && connection.getJdbcUri() != null
                && !connection.getJdbcUri().isBlank()) {
            return connection.getJdbcUri();
        }

        String host = connection.getUrl();
        int port = connection.getPort() != null && connection.getPort() > 0
                ? connection.getPort()
                : switch (connection.getType()) {
                    case SqlServer -> 1433;
                    case MySql     -> 3306;
                    case Oracle    -> 1521;
                    default        -> 5432;
                };
        String database = connection.getDatabase();

        return switch (connection.getType()) {
            case MySql -> String.format("jdbc:mysql://%s:%d/%s", host, port, database);

            case SqlServer -> {
                StringBuilder url = new StringBuilder(
                        String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, database));
                if ("Windows".equals(connection.getAuthentication())) {
                    url.append(";integratedSecurity=true");
                }
                url.append(";encrypt=true;trustServerCertificate=true");
                yield url.toString();
            }

            case Oracle -> {
                String identifier = connection.getIdentifier();
                if ("SID".equals(identifier)) {
                    yield String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, database);
                } else {
                    // ServiceName (default)
                    yield String.format("jdbc:oracle:thin:@//%s:%d/%s", host, port, database);
                }
            }

            default -> { // Postgres
                StringBuilder url = new StringBuilder(
                        String.format("jdbc:postgresql://%s:%d/%s", host, port, database));
                if (Boolean.TRUE.equals(connection.getUseSSL())) {
                    url.append("?ssl=true");
                    if (connection.getSslMode() != null) {
                        String sslMode = switch (connection.getSslMode()) {
                            case "VerifyCA" -> "verify-ca";
                            case "VerifyFull" -> "verify-full";
                            default -> connection.getSslMode().toLowerCase();
                        };
                        url.append("&sslmode=").append(sslMode);
                    }
                }
                yield url.toString();
            }
        };
    }
}
