package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.Connection.ConnectionType;
import com.nativelogix.data.migration.framework.model.project.JsonColumnMapping;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds parameterized SQL SELECT statements from table mapping definitions.
 * Supports both XML and JSON mapping types.
 *
 * <p>All identifiers are quoted to handle reserved words and mixed-case names.
 * Limit syntax varies by database vendor.</p>
 */
@Component
public class SqlQueryBuilder {

    // ── XML mappings ──────────────────────────────────────────────────────────

    public String buildRootQuery(XmlTableMapping mapping, ConnectionType dbType, int limit) {
        return buildRootQuery(mapping, dbType, limit, null);
    }

    public String buildRootQuery(XmlTableMapping mapping, ConnectionType dbType, int limit, String whereClause) {
        String columns = buildXmlSelectList(mapping);
        String table   = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return buildLimitedSelect(columns, table, whereClause, dbType, limit);
    }

    public String buildChildQuery(XmlTableMapping mapping, String childJoinCol) {
        String columns = buildXmlSelectList(mapping);
        String table   = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return "SELECT %s FROM %s WHERE %s".formatted(columns, table, quote(childJoinCol) + " = ?");
    }

    /** Batch IN query: fetches child rows for a list of parent FK values in one round trip. */
    public String buildChildBatchQuery(XmlTableMapping mapping, String childJoinCol, int batchSize) {
        String columns      = buildXmlSelectList(mapping);
        String table        = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        String placeholders = "?,".repeat(batchSize);
        placeholders        = placeholders.substring(0, placeholders.length() - 1);
        return "SELECT %s FROM %s WHERE %s IN (%s)".formatted(columns, table, quote(childJoinCol), placeholders);
    }

    /** Paginated root query used by parallel partitions (stable ordering via ORDER BY 1). */
    public String buildPagedRootQuery(XmlTableMapping mapping, ConnectionType dbType,
                                      String whereClause, long offset, long pageSize) {
        String columns = buildXmlSelectList(mapping);
        String table   = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return buildPagedSelect(columns, table, whereClause, dbType, offset, pageSize);
    }

    // ── JSON mappings ─────────────────────────────────────────────────────────

    public String buildRootQuery(JsonTableMapping mapping, ConnectionType dbType, int limit) {
        return buildRootQuery(mapping, dbType, limit, null);
    }

    public String buildRootQuery(JsonTableMapping mapping, ConnectionType dbType, int limit, String whereClause) {
        String columns = buildJsonSelectList(mapping);
        String table   = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return buildLimitedSelect(columns, table, whereClause, dbType, limit);
    }

    public String buildChildQuery(JsonTableMapping mapping, String childJoinCol) {
        String columns = buildJsonSelectList(mapping);
        String table   = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return "SELECT %s FROM %s WHERE %s".formatted(columns, table, quote(childJoinCol) + " = ?");
    }

    /** Batch IN query: fetches child rows for a list of parent FK values in one round trip. */
    public String buildChildBatchQuery(JsonTableMapping mapping, String childJoinCol, int batchSize) {
        String columns      = buildJsonSelectList(mapping);
        String table        = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        String placeholders = "?,".repeat(batchSize);
        placeholders        = placeholders.substring(0, placeholders.length() - 1);
        return "SELECT %s FROM %s WHERE %s IN (%s)".formatted(columns, table, quote(childJoinCol), placeholders);
    }

    /** Paginated root query used by parallel partitions (stable ordering via ORDER BY 1). */
    public String buildPagedRootQuery(JsonTableMapping mapping, ConnectionType dbType,
                                      String whereClause, long offset, long pageSize) {
        String columns = buildJsonSelectList(mapping);
        String table   = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return buildPagedSelect(columns, table, whereClause, dbType, offset, pageSize);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private String buildPagedSelect(String columns, String table, String whereClause,
                                    ConnectionType dbType, long offset, long pageSize) {
        String where = (whereClause != null && !whereClause.isBlank()) ? " WHERE " + whereClause : "";
        return switch (dbType) {
            // SQL Server 2012+ OFFSET/FETCH requires ORDER BY
            case SqlServer -> "SELECT %s FROM %s%s ORDER BY (SELECT NULL) OFFSET %d ROWS FETCH NEXT %d ROWS ONLY"
                    .formatted(columns, table, where, offset, pageSize);
            // Oracle 12c+ standard OFFSET/FETCH
            case Oracle    -> "SELECT %s FROM %s%s ORDER BY 1 OFFSET %d ROWS FETCH NEXT %d ROWS ONLY"
                    .formatted(columns, table, where, offset, pageSize);
            // PostgreSQL / MySQL
            default        -> "SELECT %s FROM %s%s ORDER BY 1 LIMIT %d OFFSET %d"
                    .formatted(columns, table, where, pageSize, offset);
        };
    }

    private String buildLimitedSelect(String columns, String table, String whereClause, ConnectionType dbType, int limit) {
        String where = (whereClause != null && !whereClause.isBlank()) ? " WHERE " + whereClause : "";
        return switch (dbType) {
            case SqlServer -> "SELECT TOP %d %s FROM %s%s".formatted(limit, columns, table, where);
            case Oracle    -> "SELECT %s FROM %s%s FETCH FIRST %d ROWS ONLY".formatted(columns, table, where, limit);
            default        -> "SELECT %s FROM %s%s LIMIT %d".formatted(columns, table, where, limit);
        };
    }

    private String buildXmlSelectList(XmlTableMapping mapping) {
        List<XmlColumnMapping> cols = mapping.getColumns();
        if (cols == null || cols.isEmpty()) return "*";
        return cols.stream()
                .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                .map(c -> quote(c.getSourceColumn()))
                .collect(Collectors.joining(", "));
    }

    private String buildJsonSelectList(JsonTableMapping mapping) {
        List<JsonColumnMapping> cols = mapping.getColumns();
        if (cols == null || cols.isEmpty()) return "*";
        return cols.stream()
                .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                .map(c -> quote(c.getSourceColumn()))
                .collect(Collectors.joining(", "));
    }

    private String qualifiedTable(String schema, String table) {
        if (schema != null && !schema.isBlank()) {
            // SchemaCrawler returns SQL Server schema names as "database.schema" (e.g. "AdventureWorks2022.HumanResources").
            // Split on dots and quote each part to produce valid multi-part identifiers.
            String quotedSchema = java.util.Arrays.stream(schema.split("\\."))
                    .map(this::quote)
                    .collect(Collectors.joining("."));
            return quotedSchema + "." + quote(table);
        }
        return quote(table);
    }

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
