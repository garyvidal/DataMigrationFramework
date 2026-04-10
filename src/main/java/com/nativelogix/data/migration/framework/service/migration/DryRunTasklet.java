package com.nativelogix.data.migration.framework.service.migration;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.migration.DryRunReport;
import com.nativelogix.data.migration.framework.model.project.*;
import com.nativelogix.data.migration.framework.service.JDBCConnectionService;
import com.nativelogix.data.migration.framework.service.generate.JoinResolver;
import com.nativelogix.data.migration.framework.service.generate.JsonDocumentBuilder;
import com.nativelogix.data.migration.framework.service.generate.SqlQueryBuilder;
import com.nativelogix.data.migration.framework.service.generate.XmlDocumentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Runs the full migration pipeline (cursor → child fetch → document build) against a small
 * sample of rows without writing anything to MarkLogic. Single-threaded; synchronous.
 * Returns a {@link DryRunReport} containing sample documents, pipeline errors, and timing.
 */
public class DryRunTasklet {

    private static final Logger log = LoggerFactory.getLogger(DryRunTasklet.class);

    private final MigrationJobContext ctx;
    private final JDBCConnectionService jdbcConnectionService;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JoinResolver joinResolver;
    private final XmlDocumentBuilder xmlDocumentBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;
    private final int sampleSize;

    public DryRunTasklet(MigrationJobContext ctx,
                         JDBCConnectionService jdbcConnectionService,
                         SqlQueryBuilder sqlQueryBuilder,
                         JoinResolver joinResolver,
                         XmlDocumentBuilder xmlDocumentBuilder,
                         JsonDocumentBuilder jsonDocumentBuilder,
                         int sampleSize) {
        this.ctx                   = ctx;
        this.jdbcConnectionService = jdbcConnectionService;
        this.sqlQueryBuilder       = sqlQueryBuilder;
        this.joinResolver          = joinResolver;
        this.xmlDocumentBuilder    = xmlDocumentBuilder;
        this.jsonDocumentBuilder   = jsonDocumentBuilder;
        this.sampleSize            = sampleSize;
    }

    public DryRunReport run(long totalSourceRecords) {
        DryRunReport report = new DryRunReport();
        report.setTotalSourceRecords(totalSourceRecords);

        Project project    = ctx.getProject();
        String mappingType = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";
        if (mappingType == null) mappingType = "XML";

        Connection sourceConn = ctx.getSourceConnection().getConnection();
        long t0 = System.currentTimeMillis();

        try (java.sql.Connection jdbcConn = jdbcConnectionService.openJdbcConnection(sourceConn)) {

            // ── Build cursor SQL with LIMIT so we only fetch the sample ────────
            String baseSql = buildCursorSql(mappingType, project);
            String limitedSql = applyLimit(baseSql, sourceConn.getType(), sampleSize);
            log.info("Dry-run sample SQL: {}", limitedSql);

            // ── Collect root rows ──────────────────────────────────────────────
            List<Map<String, Object>> rootBatch = new ArrayList<>(sampleSize);
            try (PreparedStatement stmt = jdbcConn.prepareStatement(
                    limitedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                stmt.setFetchSize(sampleSize);
                try (ResultSet rs = stmt.executeQuery()) {
                    String[] cols = columnNames(rs);
                    while (rs.next() && rootBatch.size() < sampleSize) {
                        rootBatch.add(toMap(rs, cols));
                    }
                }
            }
            report.setSampleSize(rootBatch.size());

            if (rootBatch.isEmpty()) {
                report.setElapsedMillis(System.currentTimeMillis() - t0);
                return report;
            }

            // ── Build documents (no-op writer — just capture content) ──────────
            NamingCase casing = project.getSettings() != null ? project.getSettings().getDefaultCasing() : null;
            List<String> docs = new ArrayList<>(rootBatch.size());

            if ("JSON".equalsIgnoreCase(mappingType)) {
                JsonDocumentModel dm   = project.getMapping().getJsonDocumentModel();
                JsonTableMapping root  = dm.getRoot();
                List<JsonTableMapping> allElements = dm.getElements() != null ? dm.getElements() : List.of();
                List<JsonTableMapping> children    = allElements.stream()
                        .filter(m -> m.getParentRef() == null).toList();
                Map<String, List<JsonTableMapping>> inlinesByParent = buildJsonInlineIndex(allElements);

                Map<String, Map<String, List<Map<String, Object>>>> level1Data =
                        batchFetchJsonChildren(jdbcConn, children, rootBatch, root, project);

                for (Map<String, Object> row : rootBatch) {
                    try {
                        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData =
                                resolveJsonChildren(jdbcConn, row, root, children,
                                        inlinesByParent, level1Data, project);
                        docs.add(jsonDocumentBuilder.build(root, row, childData, casing));
                    } catch (Exception e) {
                        report.getPipelineErrors().add("JSON build error: " + e.getMessage());
                        log.warn("Dry-run JSON build error: {}", e.getMessage());
                    }
                }

            } else {
                DocumentModel dm     = project.getMapping().getDocumentModel();
                XmlTableMapping root = dm.getRoot();
                List<XmlTableMapping> allElements = dm.getElements() != null ? dm.getElements() : List.of();
                List<XmlTableMapping> children    = allElements.stream()
                        .filter(m -> m.getParentRef() == null).toList();
                Map<String, List<XmlTableMapping>> inlinesByParent = buildXmlInlineIndex(allElements);
                List<XmlNamespace> namespaces = project.getMapping().getNamespaces();

                Map<String, Map<String, List<Map<String, Object>>>> level1Data =
                        batchFetchXmlChildren(jdbcConn, children, rootBatch, root, project);

                for (Map<String, Object> row : rootBatch) {
                    try {
                        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData =
                                resolveXmlChildren(jdbcConn, row, root, children,
                                        inlinesByParent, level1Data, project);
                        docs.add(xmlDocumentBuilder.build(root, row, childData, casing, namespaces));
                    } catch (Exception e) {
                        report.getPipelineErrors().add("XML build error: " + e.getMessage());
                        log.warn("Dry-run XML build error: {}", e.getMessage());
                    }
                }
            }

            report.setSampleDocuments(docs);

        } catch (Exception e) {
            report.getPipelineErrors().add("Pipeline failed: " + e.getMessage());
            log.error("Dry-run sample failed: {}", e.getMessage(), e);
        }

        long elapsed = System.currentTimeMillis() - t0;
        report.setElapsedMillis(elapsed);
        int built = report.getSampleDocuments().size();
        report.setEstimatedRowsPerSecond(elapsed > 0 ? built * 1000.0 / elapsed : 0);
        return report;
    }

    // ── SQL helpers ───────────────────────────────────────────────────────────

    private String buildCursorSql(String mappingType, Project project) {
        String schema, table, cols, whereClause;
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
        String sql = "SELECT " + cols + " FROM " + qualifiedTable(schema, table);
        if (whereClause != null && !whereClause.isBlank()) sql += " WHERE " + whereClause;
        return sql;
    }

    /** Wraps a SELECT with a database-appropriate row limit clause. */
    private String applyLimit(String sql, Connection.ConnectionType dbType, int limit) {
        if (dbType == null) dbType = Connection.ConnectionType.Postgres;
        return switch (dbType) {
            case SqlServer -> "SELECT TOP " + limit + sql.substring("SELECT".length());
            case Oracle    -> "SELECT * FROM (" + sql + ") WHERE ROWNUM <= " + limit;
            default        -> sql + " LIMIT " + limit;
        };
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
                    .map(this::quote).collect(Collectors.joining("."));
            return quotedSchema + "." + quote(table);
        }
        return quote(table);
    }

    private String quote(String id) { return "\"" + id.replace("\"", "\"\"") + "\""; }

    private static String[] columnNames(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        String[] names = new String[meta.getColumnCount()];
        for (int i = 0; i < names.length; i++) names[i] = meta.getColumnName(i + 1);
        return names;
    }

    private static Map<String, Object> toMap(ResultSet rs, String[] cols) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>(cols.length * 2);
        for (int i = 0; i < cols.length; i++) row.put(cols[i], rs.getObject(i + 1));
        return row;
    }

    // ── Mapping index helpers ─────────────────────────────────────────────────

    private Map<String, List<XmlTableMapping>> buildXmlInlineIndex(List<XmlTableMapping> all) {
        Map<String, List<XmlTableMapping>> idx = new LinkedHashMap<>();
        for (XmlTableMapping m : all) {
            if (m.getParentRef() != null)
                idx.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
        }
        return idx;
    }

    private Map<String, List<JsonTableMapping>> buildJsonInlineIndex(List<JsonTableMapping> all) {
        Map<String, List<JsonTableMapping>> idx = new LinkedHashMap<>();
        for (JsonTableMapping m : all) {
            if (m.getParentRef() != null
                    && ("InlineObject".equals(m.getMappingType()) || "Array".equals(m.getMappingType())))
                idx.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
        }
        return idx;
    }

    // ── Child fetch helpers ───────────────────────────────────────────────────

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchXmlChildren(
            java.sql.Connection jdbcConn, List<XmlTableMapping> children,
            List<Map<String, Object>> rootBatch, XmlTableMapping rootMapping, Project project) {

        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (XmlTableMapping child : children) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                List<Object> parentValues = rootBatch.stream()
                        .map(r -> r.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (parentValues.isEmpty()) continue;
                String sql = sqlQueryBuilder.buildChildBatchQuery(child, joinPath.childColumn(), parentValues.size());
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
                result.put(child.getId(), grouped);
            } catch (Exception e) {
                log.warn("Dry-run child fetch failed for '{}': {}", child.getXmlName(), e.getMessage());
                result.put(child.getId(), Map.of());
            }
        }
        return result;
    }

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchJsonChildren(
            java.sql.Connection jdbcConn, List<JsonTableMapping> children,
            List<Map<String, Object>> rootBatch, JsonTableMapping rootMapping, Project project) {

        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (JsonTableMapping child : children) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                List<Object> parentValues = rootBatch.stream()
                        .map(r -> r.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (parentValues.isEmpty()) continue;
                String sql = sqlQueryBuilder.buildChildBatchQuery(child, joinPath.childColumn(), parentValues.size());
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
                result.put(child.getId(), grouped);
            } catch (Exception e) {
                log.warn("Dry-run child fetch failed for '{}': {}", child.getJsonName(), e.getMessage());
                result.put(child.getId(), Map.of());
            }
        }
        return result;
    }

    // ── Child resolution helpers ──────────────────────────────────────────────

    private Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> resolveXmlChildren(
            java.sql.Connection jdbcConn, Map<String, Object> rootRow,
            XmlTableMapping rootMapping, List<XmlTableMapping> children,
            Map<String, List<XmlTableMapping>> inlinesByParent,
            Map<String, Map<String, List<Map<String, Object>>>> level1Data,
            Project project) throws Exception {

        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        for (XmlTableMapping child : children) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                Object parentVal = rootRow.get(joinPath.parentColumn());
                if (parentVal == null) { childData.put(child, List.of()); continue; }
                String key = String.valueOf(parentVal);
                List<Map<String, Object>> rows = level1Data
                        .getOrDefault(child.getId(), Map.of()).getOrDefault(key, List.of());
                childData.put(child, toXmlMappedRows(jdbcConn, child, rows, inlinesByParent, project));
            } catch (Exception e) {
                childData.put(child, List.of());
            }
        }
        return childData;
    }

    private Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> resolveJsonChildren(
            java.sql.Connection jdbcConn, Map<String, Object> rootRow,
            JsonTableMapping rootMapping, List<JsonTableMapping> children,
            Map<String, List<JsonTableMapping>> inlinesByParent,
            Map<String, Map<String, List<Map<String, Object>>>> level1Data,
            Project project) throws Exception {

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        for (JsonTableMapping child : children) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, child, project);
                Object parentVal = rootRow.get(joinPath.parentColumn());
                if (parentVal == null) { childData.put(child, List.of()); continue; }
                String key = String.valueOf(parentVal);
                List<Map<String, Object>> rows = level1Data
                        .getOrDefault(child.getId(), Map.of()).getOrDefault(key, List.of());
                childData.put(child, toJsonMappedRows(jdbcConn, child, rows, inlinesByParent, project));
            } catch (Exception e) {
                childData.put(child, List.of());
            }
        }
        return childData;
    }

    private List<XmlDocumentBuilder.MappedRow> toXmlMappedRows(
            java.sql.Connection jdbcConn, XmlTableMapping parentMapping,
            List<Map<String, Object>> rows, Map<String, List<XmlTableMapping>> inlinesByParent,
            Project project) throws Exception {

        List<XmlTableMapping> inlines = inlinesByParent.get(parentMapping.getId());
        Map<String, Map<String, List<Map<String, Object>>>> inlineData =
                batchFetchXmlInlines(jdbcConn, inlines, rows, parentMapping, project);

        List<XmlDocumentBuilder.MappedRow> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> inlineChildData = new LinkedHashMap<>();
            if (inlines != null) {
                for (XmlTableMapping inline : inlines) {
                    try {
                        JoinResolver.JoinPath jp = joinResolver.resolve(parentMapping, inline, project);
                        Object joinVal = row.get(jp.parentColumn());
                        if (joinVal == null) { inlineChildData.put(inline, List.of()); continue; }
                        List<Map<String, Object>> inlineRows = inlineData
                                .getOrDefault(inline.getId(), Map.of())
                                .getOrDefault(String.valueOf(joinVal), List.of());
                        inlineChildData.put(inline, inlineRows.stream()
                                .map(r -> new XmlDocumentBuilder.MappedRow(r, Map.of()))
                                .collect(Collectors.toList()));
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
            java.sql.Connection jdbcConn, JsonTableMapping parentMapping,
            List<Map<String, Object>> rows, Map<String, List<JsonTableMapping>> inlinesByParent,
            Project project) throws Exception {

        List<JsonTableMapping> inlines = inlinesByParent.get(parentMapping.getId());
        Map<String, Map<String, List<Map<String, Object>>>> inlineData =
                batchFetchJsonInlines(jdbcConn, inlines, rows, parentMapping, project);

        List<JsonDocumentBuilder.MappedRow> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> inlineChildData = new LinkedHashMap<>();
            if (inlines != null) {
                for (JsonTableMapping inline : inlines) {
                    try {
                        JoinResolver.JoinPath jp = joinResolver.resolve(parentMapping, inline, project);
                        Object joinVal = row.get(jp.parentColumn());
                        if (joinVal == null) { inlineChildData.put(inline, List.of()); continue; }
                        List<Map<String, Object>> inlineRows = inlineData
                                .getOrDefault(inline.getId(), Map.of())
                                .getOrDefault(String.valueOf(joinVal), List.of());
                        inlineChildData.put(inline, inlineRows.stream()
                                .map(r -> new JsonDocumentBuilder.MappedRow(r, Map.of()))
                                .collect(Collectors.toList()));
                    } catch (Exception e) {
                        inlineChildData.put(inline, List.of());
                    }
                }
            }
            result.add(new JsonDocumentBuilder.MappedRow(row, inlineChildData));
        }
        return result;
    }

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchXmlInlines(
            java.sql.Connection jdbcConn, List<XmlTableMapping> inlines,
            List<Map<String, Object>> parentRows, XmlTableMapping parentMapping, Project project) {

        if (inlines == null || inlines.isEmpty() || parentRows.isEmpty()) return Map.of();
        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (XmlTableMapping inline : inlines) {
            try {
                JoinResolver.JoinPath jp = joinResolver.resolve(parentMapping, inline, project);
                List<Object> fkValues = parentRows.stream().map(r -> r.get(jp.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (fkValues.isEmpty()) continue;
                String sql = sqlQueryBuilder.buildChildBatchQuery(inline, jp.childColumn(), fkValues.size());
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < fkValues.size(); i++) stmt.setObject(i + 1, fkValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs, cols);
                            grouped.computeIfAbsent(String.valueOf(row.get(jp.childColumn())),
                                    k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                result.put(inline.getId(), grouped);
            } catch (Exception e) {
                log.warn("Dry-run inline fetch failed for '{}': {}", inline.getXmlName(), e.getMessage());
                result.put(inline.getId(), Map.of());
            }
        }
        return result;
    }

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchJsonInlines(
            java.sql.Connection jdbcConn, List<JsonTableMapping> inlines,
            List<Map<String, Object>> parentRows, JsonTableMapping parentMapping, Project project) {

        if (inlines == null || inlines.isEmpty() || parentRows.isEmpty()) return Map.of();
        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        for (JsonTableMapping inline : inlines) {
            try {
                JoinResolver.JoinPath jp = joinResolver.resolve(parentMapping, inline, project);
                List<Object> fkValues = parentRows.stream().map(r -> r.get(jp.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (fkValues.isEmpty()) continue;
                String sql = sqlQueryBuilder.buildChildBatchQuery(inline, jp.childColumn(), fkValues.size());
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < fkValues.size(); i++) stmt.setObject(i + 1, fkValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        String[] cols = columnNames(rs);
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs, cols);
                            grouped.computeIfAbsent(String.valueOf(row.get(jp.childColumn())),
                                    k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                result.put(inline.getId(), grouped);
            } catch (Exception e) {
                log.warn("Dry-run inline fetch failed for '{}': {}", inline.getJsonName(), e.getMessage());
                result.put(inline.getId(), Map.of());
            }
        }
        return result;
    }
}
