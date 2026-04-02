package com.nativelogix.data.migration.framework.service;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.relational.*;
import com.nativelogix.data.migration.framework.model.requests.SchemaAnalysisRequest;
import com.nativelogix.data.migration.framework.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.*;
import schemacrawler.schemacrawler.*;
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSourceBuilder;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class SchemaService {

    // Oracle built-in and system schemas to exclude from schema analysis results.
    // Filtered in Java rather than via SchemaCrawler includeSchemas, because the
    // Oracle SchemaCrawler plugin converts the inclusion rule into a SQL LIKE clause
    // and rejects non-LIKE regex patterns (negative lookaheads, anchors, etc.).
    private static final Set<String> ORACLE_SYSTEM_SCHEMAS = Set.of(
            "SYS", "SYSTEM", "SYSMAN", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC",
            "DBSNMP", "OUTLN", "XDB", "ANONYMOUS", "APPQOSSYS", "MDSYS", "CTXSYS",
            "ORDSYS", "EXFSYS", "WMSYS", "LBACSYS", "OLAPSYS", "ORDPLUGINS",
            "DMSYS", "ORACLE_OCM", "MDDATA", "AUDSYS", "DBSFWUSER", "DVF", "DVSYS",
            "GGSYS", "GSMADMIN_INTERNAL", "GSMCATUSER", "GSMROOTUSER", "GSMUSER",
            "OJVMSYS", "REMOTE_SCHEDULER_AGENT", "ORDDATA", "OPS$ORACLE"
    );

    private final ConnectionRepository connectionRepository;
    private final PasswordEncryptionService encryptionService;

    public DbDatabase analyzeSchema(SchemaAnalysisRequest request) {
        final Connection connection = resolveConnection(request);
        validateConnection(connection);

        final LimitOptions limitOptions = buildLimitOptions(connection);
        final LoadOptions loadOptions = LoadOptionsBuilder
                .builder()
                .withSchemaInfoLevel(SchemaInfoLevelBuilder.standard())
                .build();
        final SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder
                .newSchemaCrawlerOptions()
                .withLimitOptions(limitOptions)
                .withLoadOptions(loadOptions);

        final DatabaseConnectionSource dataSource = getDataSource(connection);
        final Catalog catalog;
        try {
            catalog = SchemaCrawlerUtility.getCatalog(dataSource, options);
        } catch (Exception e) {
            log.debug("Schema analysis failed for {}", buildJdbcUrl(connection), e);
            throw new RuntimeException(translateConnectionError(connection, e), e);
        }

        DbDatabase database = new DbDatabase();
        Map<String, DbSchema> schemas = new LinkedHashMap<>();

        boolean isOracle = connection.getType() == Connection.ConnectionType.Oracle;

        for (final Schema schema : catalog.getSchemas()) {
            if (isOracle && ORACLE_SYSTEM_SCHEMAS.contains(schema.getName().toUpperCase())) {
                log.debug("Skipping Oracle system schema: '{}'", schema.getName());
                continue;
            }
            log.debug("Found schema: name='{}' fullName='{}'", schema.getName(), schema.getFullName());
            DbSchema dbSchema = new DbSchema();
            dbSchema.setName(schema.getName());
            dbSchema.setFullName(schema.getFullName());
            Map<String, DbTable> tables = new LinkedHashMap<>();
            if (request.isIncludeTables()) {
                List<Table> sortedTables = catalog.getTables(schema)
                        .stream()
                        .sorted(Comparator.comparing(Table::getName))
                        .collect(Collectors.toList());
                log.debug("Schema '{}' has {} tables", schema.getFullName(), sortedTables.size());
                for (final Table table : sortedTables) {
                    DbTable dbTable = new DbTable();
                    dbTable.setTableName(table.getName());
                    dbTable.setFullName(table.getFullName());
                    dbTable.setSchema(schema.getName());
                    if (request.isIncludeColumns()) {
                        dbTable.setColumns(getColumns(table));
                    }
                    if (request.isIncludeRelationships()) {
                        dbTable.setRelationships(getRelationships(table));
                    }
                    tables.put(table.getName(), dbTable);
                }
            }
            schemas.put(schema.getFullName(), dbSchema);
            dbSchema.setTables(tables);
        }

        database.setSchemas(schemas);
        return database;
    }

    private LimitOptions buildLimitOptions(Connection connection) {
        if (connection.getType() == Connection.ConnectionType.Oracle) {
            // Oracle: do NOT set includeSchemas — the Oracle SchemaCrawler plugin
            // converts the inclusion rule to a SQL LIKE clause, which rejects regex
            // syntax (anchors, negative lookaheads, etc.) and returns no tables.
            // System schema exclusion is applied in Java in the analyzeSchema loop.
            return LimitOptionsBuilder.builder().build();
        }
        // Other databases: restrict to TABLE/VIEW and exclude system schemas.
        return LimitOptionsBuilder.builder()
                .tableTypes("TABLE,VIEW")
                .includeSchemas(new RegularExpressionInclusionRule(
                        "^(?i)(?!master$|tempdb$|model$|msdb$|dbo$|sys$|pg_catalog$|information_schema$).*"
                ))
                .build();
    }

    /**
     * Resolves the Connection to use.
     * If connectionId is set, look up the stored (already-decrypted) credentials from the repository.
     * Otherwise use the connection from the request, decrypting any ENC:-prefixed password.
     */
    private Connection resolveConnection(SchemaAnalysisRequest request) {
        if (request.getConnectionId() != null && !request.getConnectionId().isBlank()) {
            return connectionRepository.findAll().stream()
                    .filter(sc -> request.getConnectionId().equals(sc.getId()))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Connection not found: " + request.getConnectionId()))
                    .getConnection();
        }
        Connection conn = request.getConnection();
        if (conn != null && conn.getPassword() != null) {
            Connection decrypted = new Connection();
            decrypted.setType(conn.getType());
            decrypted.setUrl(conn.getUrl());
            decrypted.setPort(conn.getPort());
            decrypted.setDatabase(conn.getDatabase());
            decrypted.setUserName(conn.getUserName());
            decrypted.setPassword(encryptionService.decrypt(conn.getPassword()));
            decrypted.setEnterUriManually(conn.getEnterUriManually());
            decrypted.setJdbcUri(conn.getJdbcUri());
            decrypted.setAuthentication(conn.getAuthentication());
            decrypted.setIdentifier(conn.getIdentifier());
            decrypted.setPdbName(conn.getPdbName());
            decrypted.setUseSSL(conn.getUseSSL());
            decrypted.setSslMode(conn.getSslMode());
            return decrypted;
        }
        return conn;
    }

    private DatabaseConnectionSource getDataSource(Connection connection) {
        return DatabaseConnectionSourceBuilder
                .builder(buildJdbcUrl(connection))
                .withUserCredentials(new MultiUseUserCredentials(connection.getUserName(), connection.getPassword()))
                .build();
    }

    private String buildJdbcUrl(Connection connection) {
        String host = connection.getUrl();
        int port = connection.getPort();
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
                // ServiceName uses thin:@//host:port/serviceName; SID uses thin:@host:port:sid
                String pdb = connection.getPdbName();
                String dbName = (pdb != null && !pdb.isBlank()) ? pdb : database;
                if ("SID".equalsIgnoreCase(connection.getIdentifier())) {
                    yield String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, dbName);
                } else {
                    yield String.format("jdbc:oracle:thin:@//%s:%d/%s", host, port, dbName);
                }
            }
            default -> String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        };
    }

    private void validateConnection(Connection connection) {
        if (connection.getType() == null) {
            throw new RuntimeException("Database type is required.");
        }
        if (connection.getUrl() == null || connection.getUrl().isBlank()) {
            throw new RuntimeException("Host is required.");
        }
        if (connection.getPort() == null || connection.getPort() <= 0) {
            throw new RuntimeException("A valid port number is required.");
        }
        if (connection.getUserName() == null || connection.getUserName().isBlank()) {
            throw new RuntimeException("Username is required.");
        }
        if (connection.getPassword() == null || connection.getPassword().isBlank()) {
            throw new RuntimeException("Password is required. If you saved this connection before a server restart, the password may have been cleared — please re-enter it.");
        }
        if (connection.getDatabase() == null || connection.getDatabase().isBlank()) {
            String label = connection.getType() == Connection.ConnectionType.Oracle ? "service name or SID" : "database name";
            throw new RuntimeException("The " + label + " is required.");
        }
    }

    private String translateConnectionError(Connection connection, Throwable ex) {
        String host = connection.getUrl();
        int port = connection.getPort();
        String user = connection.getUserName();
        String db   = connection.getDatabase();
        Connection.ConnectionType type = connection.getType();

        // Walk the cause chain to find the most informative message
        Throwable cause = ex;
        while (cause != null) {
            String msg = cause.getMessage();
            if (msg == null) { cause = cause.getCause(); continue; }
            String lower = msg.toLowerCase();

            // ── Network ───────────────────────────────────────────────────────
            if (cause instanceof UnknownHostException || lower.contains("unknown host")) {
                return String.format("Unknown host '%s' — verify the hostname is correct and reachable.", host);
            }
            if (cause instanceof ConnectException || lower.contains("connection refused")) {
                return String.format("Connection refused at %s:%d — the server may be down or the port is incorrect.", host, port);
            }
            if (cause instanceof SocketTimeoutException || lower.contains("timed out") || lower.contains("timeout")) {
                return String.format("Connection to %s:%d timed out — check host, port, and firewall rules.", host, port);
            }

            // ── Authentication ────────────────────────────────────────────────
            if (lower.contains("ora-01017") || lower.contains("invalid username/password")) {
                return String.format("Authentication failed for user '%s' — check username and password.", user);
            }
            if (lower.contains("password authentication failed") || lower.contains("login failed")) {
                return String.format("Authentication failed for user '%s' — check username and password.", user);
            }
            if (lower.contains("ora-01045") || lower.contains("lacks create session")) {
                return String.format("User '%s' does not have the CREATE SESSION privilege — contact your DBA.", user);
            }
            if (lower.contains("ora-28000") || lower.contains("account is locked")) {
                return String.format("Account '%s' is locked — contact your DBA to unlock it.", user);
            }

            // ── Oracle service / SID ──────────────────────────────────────────
            if (lower.contains("ora-12514") || lower.contains("listener does not currently know of service")) {
                return String.format("Oracle service name '%s' not found on %s:%d — check the service name.", db, host, port);
            }
            if (lower.contains("ora-12505") || lower.contains("listener does not currently know of sid")) {
                return String.format("Oracle SID '%s' is not registered with the listener on %s:%d — check the SID.", db, host, port);
            }
            if (lower.contains("ora-12541") || lower.contains("no listener")) {
                return String.format("No Oracle listener found at %s:%d — verify the listener is running.", host, port);
            }
            if (lower.contains("ora-12519") || lower.contains("no appropriate service handler")) {
                return String.format("Oracle listener on %s:%d has no available handlers for service '%s' — the database may be at max connections.", host, port, db);
            }

            // ── SQL Server ────────────────────────────────────────────────────
            if (lower.contains("cannot open database")) {
                return String.format("SQL Server database '%s' does not exist or user '%s' cannot access it.", db, user);
            }

            // ── PostgreSQL ────────────────────────────────────────────────────
            if (lower.contains("database") && lower.contains("does not exist")) {
                return String.format("PostgreSQL database '%s' does not exist on %s:%d.", db, host, port);
            }

            // ── SSL ───────────────────────────────────────────────────────────
            if (lower.contains("ssl") || lower.contains("certificate")) {
                return String.format("SSL/TLS error connecting to %s:%d — check SSL settings for this connection.", host, port);
            }

            cause = cause.getCause();
        }

        // Fallback: return the raw message of the outermost exception
        String raw = ex.getMessage();
        return (raw != null && !raw.isBlank())
                ? String.format("Failed to connect to %s %s:%d — %s", type, host, port, raw)
                : String.format("Failed to connect to %s %s:%d — an unexpected error occurred.", type, host, port);
    }

    private List<DbRelationship> getRelationships(Table table) {
        List<DbRelationship> relationships = new ArrayList<>();
        for (ForeignKey fk : table.getForeignKeys()) {
            for (ColumnReference ref : fk.getColumnReferences()) {
                DbRelationship relationship = new DbRelationship();
                relationship.setFromColumn(ref.getForeignKeyColumn().getName());
                relationship.setToTable(ref.getPrimaryKeyColumn().getParent().getFullName());
                relationship.setToColumn(ref.getPrimaryKeyColumn().getName());
                relationships.add(relationship);
            }
        }
        return relationships;
    }

    private Map<String, DbColumn> getColumns(Table table) {
        Map<String, DbColumn> columns = new LinkedHashMap<>();
        List<Column> sortedColumns = table.getColumns()
                .stream()
                .sorted(Comparator.comparingInt(Column::getOrdinalPosition))
                .toList();
        for (final Column column : sortedColumns) {
            DbColumn dbColumn = new DbColumn();
            dbColumn.setFullName(column.getFullName());
            dbColumn.setName(column.getName());
            dbColumn.setType(column.getType().getJavaSqlType().getName());
            dbColumn.setPosition(column.getOrdinalPosition());
            dbColumn.setSequence(column.isAutoIncremented());
            dbColumn.setPrimaryKey(column.isPartOfPrimaryKey());
            DbColumnType dbType = new DbColumnType();
            dbType.setColumnType(column.getType().getDatabaseSpecificTypeName());
            dbType.setPrecision(column.getType().getPrecision());
            dbType.setScale(column.getType().getMaximumScale());
            dbColumn.setColumnType(dbType);
            Column refColumn = column.getReferencedColumn();
            if (refColumn != null && column.isPartOfForeignKey()) {
                DbForeignKey fkey = new DbForeignKey();
                fkey.setFullName(refColumn.getFullName());
                fkey.setName(refColumn.getName());
                dbColumn.setForeignKey(fkey);
            }
            columns.put(dbColumn.getName(), dbColumn);
        }
        return columns;
    }
}
