package com.nativelogix.rdbms2marklogic.service;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.relational.*;
import com.nativelogix.rdbms2marklogic.model.requests.SchemaAnalysisRequest;
import org.springframework.stereotype.Service;
import schemacrawler.schema.*;
import schemacrawler.schemacrawler.*;
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSourceBuilder;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class SchemaService {

    public DbDatabase analyzeSchema(SchemaAnalysisRequest request) {
        final LimitOptions limitOptions = LimitOptionsBuilder.builder().build();
        final LoadOptions loadOptions = LoadOptionsBuilder
                .builder()
                .withSchemaInfoLevel(SchemaInfoLevelBuilder.standard())
                .build();
        final SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder
                .newSchemaCrawlerOptions()
                .withLimitOptions(limitOptions)
                .withLoadOptions(loadOptions);

        final DatabaseConnectionSource dataSource = getDataSource(request.getConnection());
        final Catalog catalog = SchemaCrawlerUtility.getCatalog(dataSource, options);

        DbDatabase database = new DbDatabase();
        Map<String, DbSchema> schemas = new HashMap<>();

        for (final Schema schema : catalog.getSchemas()) {
            DbSchema dbSchema = new DbSchema();
            dbSchema.setName(schema.getName());
            Map<String, DbTable> tables = new LinkedHashMap<>();
            if (request.isIncludeTables()) {
                List<Table> sortedTables = catalog.getTables(schema)
                        .stream()
                        .sorted(Comparator.comparing(Table::getName))
                        .collect(Collectors.toList());
                for (final Table table : sortedTables) {
                    DbTable dbTable = new DbTable();
                    dbTable.setTableName(table.getName());
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
            case SqlServer -> String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, database);
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
                relationship.setToTable(ref.getPrimaryKeyColumn().getParent().getName());
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
                fkey.setName(refColumn.getName());
                dbColumn.setForeignKey(fkey);
            }
            columns.put(dbColumn.getName(), dbColumn);
        }
        return columns;
    }
}
