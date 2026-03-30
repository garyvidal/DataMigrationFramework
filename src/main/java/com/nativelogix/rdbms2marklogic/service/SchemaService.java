package com.nativelogix.rdbms2marklogic.service;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.relational.*;
import com.nativelogix.rdbms2marklogic.model.requests.SchemaAnalysisRequest;
import com.nativelogix.rdbms2marklogic.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.*;
import schemacrawler.schemacrawler.*;
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSourceBuilder;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class SchemaService {

    private final ConnectionRepository connectionRepository;
    private final PasswordEncryptionService encryptionService;

    public DbDatabase analyzeSchema(SchemaAnalysisRequest request) {
        final LimitOptions limitOptions = LimitOptionsBuilder.builder()
                .tableTypes("TABLE,VIEW")
                .includeSchemas(new RegularExpressionInclusionRule(
                        "^(?i)(?!master$|tempdb$|model$|msdb$|dbo$).*"
                ))
                .build();

        final LoadOptions loadOptions = LoadOptionsBuilder
                .builder()
                .withSchemaInfoLevel(SchemaInfoLevelBuilder.standard())
                .build();
        final SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder
                .newSchemaCrawlerOptions()
                .withLimitOptions(limitOptions)
                .withLoadOptions(loadOptions);

        final DatabaseConnectionSource dataSource = getDataSource(resolveConnection(request));
        final Catalog catalog = SchemaCrawlerUtility.getCatalog(dataSource, options);

        DbDatabase database = new DbDatabase();
        Map<String, DbSchema> schemas = new HashMap<>();

        for (final Schema schema : catalog.getSchemas()) {
            DbSchema dbSchema = new DbSchema();
            dbSchema.setName(schema.getName());
            dbSchema.setFullName(schema.getFullName());
            Map<String, DbTable> tables = new LinkedHashMap<>();
            if (request.isIncludeTables()) {
                List<Table> sortedTables = catalog.getTables(schema)
                        .stream()
                        .sorted(Comparator.comparing(Table::getName))
                        .collect(Collectors.toList());
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
                schemas.put(schema.getFullName(), dbSchema);
                dbSchema.setTables(tables);
            }
        }

        database.setSchemas(schemas);
        return database;
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
            case Oracle -> String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, database);
            default -> String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        };
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
