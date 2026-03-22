package com.nativelogix.rdbms2marklogic.service.migration;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.SavedConnection;
import com.nativelogix.rdbms2marklogic.model.project.*;
import com.nativelogix.rdbms2marklogic.service.JDBCConnectionService;
import com.nativelogix.rdbms2marklogic.service.generate.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;

import java.sql.*;
import java.util.*;

/**
 * Spring Batch ItemReader that streams root rows from the RDBMS using a JDBC cursor,
 * fetches child rows for each, and builds the full document string.
 * <p>
 * The JDBC connection is opened once on {@link #open(ExecutionContext)} and closed on {@link #close()}.
 */
public class RdbmsDocumentReader implements ItemStreamReader<DocumentBuildResult> {

    private static final Logger log = LoggerFactory.getLogger(RdbmsDocumentReader.class);

    private final MigrationJobContext ctx;
    private final JDBCConnectionService jdbcConnectionService;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JoinResolver joinResolver;
    private final XmlDocumentBuilder xmlDocumentBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;

    private java.sql.Connection jdbcConn;
    private ResultSet rootRs;
    private PreparedStatement rootStmt;

    private int rowIndex = 0;
    private String mappingType;

    // For XML
    private XmlTableMapping xmlRootMapping;
    private List<XmlTableMapping> xmlRootLevelMappings;
    private Map<String, List<XmlTableMapping>> xmlInlinesByParentId;

    // For JSON
    private JsonTableMapping jsonRootMapping;
    private List<JsonTableMapping> jsonRootLevelMappings;
    private Map<String, List<JsonTableMapping>> jsonInlinesByParentId;

    public RdbmsDocumentReader(MigrationJobContext ctx,
                                JDBCConnectionService jdbcConnectionService,
                                SqlQueryBuilder sqlQueryBuilder,
                                JoinResolver joinResolver,
                                XmlDocumentBuilder xmlDocumentBuilder,
                                JsonDocumentBuilder jsonDocumentBuilder) {
        this.ctx = ctx;
        this.jdbcConnectionService = jdbcConnectionService;
        this.sqlQueryBuilder = sqlQueryBuilder;
        this.joinResolver = joinResolver;
        this.xmlDocumentBuilder = xmlDocumentBuilder;
        this.jsonDocumentBuilder = jsonDocumentBuilder;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        Project project = ctx.getProject();
        mappingType = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";
        if (mappingType == null) mappingType = "XML";

        String connectionName = project.getConnectionName();
        SavedConnection savedConn = jdbcConnectionService.getConnection(connectionName)
                .orElseThrow(() -> new IllegalStateException("Connection not found: " + connectionName));
        Connection conn = savedConn.getConnection();
        Connection.ConnectionType dbType = conn.getType() != null ? conn.getType() : Connection.ConnectionType.Postgres;

        try {
            jdbcConn = jdbcConnectionService.openJdbcConnection(conn);

            if ("JSON".equalsIgnoreCase(mappingType)) {
                JsonDocumentModel docModel = project.getMapping().getJsonDocumentModel();
                jsonRootMapping = docModel.getRoot();
                List<JsonTableMapping> allMappings = docModel.getElements() != null ? docModel.getElements() : List.of();
                jsonInlinesByParentId = new LinkedHashMap<>();
                for (JsonTableMapping m : allMappings) {
                    if (m.getParentRef() != null && ("InlineObject".equals(m.getMappingType()) || "Array".equals(m.getMappingType()))) {
                        jsonInlinesByParentId.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
                    }
                }
                jsonRootLevelMappings = allMappings.stream().filter(m -> m.getParentRef() == null).toList();
                String sql = buildUnlimitedRootQueryJson(jsonRootMapping, dbType);
                rootStmt = jdbcConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            } else {
                DocumentModel docModel = project.getMapping().getDocumentModel();
                xmlRootMapping = docModel.getRoot();
                List<XmlTableMapping> allMappings = docModel.getElements() != null ? docModel.getElements() : List.of();
                xmlInlinesByParentId = new LinkedHashMap<>();
                for (XmlTableMapping m : allMappings) {
                    if (m.getParentRef() != null) {
                        xmlInlinesByParentId.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
                    }
                }
                xmlRootLevelMappings = allMappings.stream().filter(m -> m.getParentRef() == null).toList();
                String sql = buildUnlimitedRootQueryXml(xmlRootMapping, dbType);
                rootStmt = jdbcConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            }

            rootRs = rootStmt.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException("Failed to open RDBMS cursor: " + e.getMessage(), e);
        }
    }

    @Override
    public DocumentBuildResult read() throws Exception {
        if (rootRs == null || !rootRs.next()) return null;

        rowIndex++;
        Project project = ctx.getProject();
        NamingCase casing = project.getSettings() != null ? project.getSettings().getDefaultCasing() : null;
        Map<String, Object> rootRow = toMap(rootRs);
        String uri = buildUri(rootRow, rowIndex);

        if ("JSON".equalsIgnoreCase(mappingType)) {
            Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
            for (JsonTableMapping childMapping : jsonRootLevelMappings) {
                try {
                    JoinResolver.JoinPath joinPath = joinResolver.resolve(jsonRootMapping, childMapping, project);
                    Object parentValue = rootRow.get(joinPath.parentColumn());
                    if (parentValue == null) { childData.put(childMapping, List.of()); continue; }
                    List<JsonDocumentBuilder.MappedRow> rows = queryJsonMappedRows(childMapping, joinPath.childColumn(), parentValue, project);
                    childData.put(childMapping, rows);
                } catch (Exception e) {
                    log.warn("Child join failed for '{}': {}", childMapping.getJsonName(), e.getMessage());
                    childData.put(childMapping, List.of());
                }
            }
            String content = jsonDocumentBuilder.build(jsonRootMapping, rootRow, childData, casing);
            return new DocumentBuildResult(uri + ".json", content, "JSON");
        } else {
            Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
            for (XmlTableMapping childMapping : xmlRootLevelMappings) {
                try {
                    JoinResolver.JoinPath joinPath = joinResolver.resolve(xmlRootMapping, childMapping, project);
                    Object parentValue = rootRow.get(joinPath.parentColumn());
                    if (parentValue == null) { childData.put(childMapping, List.of()); continue; }
                    List<XmlDocumentBuilder.MappedRow> rows = queryXmlMappedRows(childMapping, joinPath.childColumn(), parentValue, project);
                    childData.put(childMapping, rows);
                } catch (Exception e) {
                    log.warn("Child join failed for '{}': {}", childMapping.getXmlName(), e.getMessage());
                    childData.put(childMapping, List.of());
                }
            }
            List<XmlNamespace> namespaces = project.getMapping().getNamespaces();
            String content = xmlDocumentBuilder.build(xmlRootMapping, rootRow, childData, casing, namespaces);
            return new DocumentBuildResult(uri + ".xml", content, "XML");
        }
    }

    @Override
    public void close() {
        try { if (rootRs != null) rootRs.close(); } catch (Exception ignored) {}
        try { if (rootStmt != null) rootStmt.close(); } catch (Exception ignored) {}
        try { if (jdbcConn != null) jdbcConn.close(); } catch (Exception ignored) {}
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private String buildUri(Map<String, Object> rootRow, int idx) {
        String dirPath = ctx.getDirectoryPath();
        if (dirPath == null || dirPath.isBlank()) dirPath = "/";
        if (!dirPath.endsWith("/")) dirPath += "/";

        // Variable substitution: {rootElement}, {index}
        String rootElement = "JSON".equalsIgnoreCase(mappingType)
                ? (jsonRootMapping != null ? jsonRootMapping.getJsonName() : "doc")
                : (xmlRootMapping != null ? xmlRootMapping.getXmlName() : "doc");

        String dir = dirPath.replace("{rootElement}", rootElement).replace("{index}", String.valueOf(idx));

        // Try to use primary key value in filename
        String pkValue = findPkValue(rootRow);
        String filename = pkValue != null ? pkValue : String.valueOf(idx);
        return dir + filename;
    }

    private String findPkValue(Map<String, Object> row) {
        // Look for common PK column names
        for (String key : row.keySet()) {
            String lower = key.toLowerCase();
            if (lower.equals("id") || lower.endsWith("_id") || lower.endsWith("id")) {
                Object val = row.get(key);
                if (val != null) return val.toString();
            }
        }
        return null;
    }

    private List<JsonDocumentBuilder.MappedRow> queryJsonMappedRows(
            JsonTableMapping mapping, String childCol, Object joinValue, Project project) throws Exception {
        String sql = sqlQueryBuilder.buildChildQuery(mapping, childCol);
        List<JsonDocumentBuilder.MappedRow> result = new ArrayList<>();
        try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
            stmt.setObject(1, joinValue);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = toMap(rs);
                    Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> inlineChildren =
                            queryJsonInlineChildren(mapping, row, project);
                    result.add(new JsonDocumentBuilder.MappedRow(row, inlineChildren));
                }
            }
        }
        return result;
    }

    private Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> queryJsonInlineChildren(
            JsonTableMapping parentMapping, Map<String, Object> parentRow, Project project) throws Exception {
        List<JsonTableMapping> inlines = jsonInlinesByParentId.get(parentMapping.getId());
        if (inlines == null || inlines.isEmpty()) return Map.of();
        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> result = new LinkedHashMap<>();
        for (JsonTableMapping inline : inlines) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);
                Object joinValue = parentRow.get(joinPath.parentColumn());
                if (joinValue == null) { result.put(inline, List.of()); continue; }
                result.put(inline, queryJsonMappedRows(inline, joinPath.childColumn(), joinValue, project));
            } catch (Exception e) {
                result.put(inline, List.of());
            }
        }
        return result;
    }

    private List<XmlDocumentBuilder.MappedRow> queryXmlMappedRows(
            XmlTableMapping mapping, String childCol, Object joinValue, Project project) throws Exception {
        String sql = sqlQueryBuilder.buildChildQuery(mapping, childCol);
        List<XmlDocumentBuilder.MappedRow> result = new ArrayList<>();
        try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
            stmt.setObject(1, joinValue);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = toMap(rs);
                    Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> inlineChildren =
                            queryXmlInlineChildren(mapping, row, project);
                    result.add(new XmlDocumentBuilder.MappedRow(row, inlineChildren));
                }
            }
        }
        return result;
    }

    private Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> queryXmlInlineChildren(
            XmlTableMapping parentMapping, Map<String, Object> parentRow, Project project) throws Exception {
        List<XmlTableMapping> inlines = xmlInlinesByParentId.get(parentMapping.getId());
        if (inlines == null || inlines.isEmpty()) return Map.of();
        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> result = new LinkedHashMap<>();
        for (XmlTableMapping inline : inlines) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);
                Object joinValue = parentRow.get(joinPath.parentColumn());
                if (joinValue == null) { result.put(inline, List.of()); continue; }
                result.put(inline, queryXmlMappedRows(inline, joinPath.childColumn(), joinValue, project));
            } catch (Exception e) {
                result.put(inline, List.of());
            }
        }
        return result;
    }

    private String buildUnlimitedRootQueryJson(JsonTableMapping mapping, Connection.ConnectionType dbType) {
        // Build SELECT without LIMIT for full batch processing
        String cols = mapping.getColumns() == null || mapping.getColumns().isEmpty() ? "*" :
                mapping.getColumns().stream()
                        .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                        .map(c -> "\"" + c.getSourceColumn().replace("\"", "\"\"") + "\"")
                        .reduce((a, b) -> a + ", " + b).orElse("*");
        String table = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return "SELECT " + cols + " FROM " + table;
    }

    private String buildUnlimitedRootQueryXml(XmlTableMapping mapping, Connection.ConnectionType dbType) {
        String cols = mapping.getColumns() == null || mapping.getColumns().isEmpty() ? "*" :
                mapping.getColumns().stream()
                        .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                        .map(c -> "\"" + c.getSourceColumn().replace("\"", "\"\"") + "\"")
                        .reduce((a, b) -> a + ", " + b).orElse("*");
        String table = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        return "SELECT " + cols + " FROM " + table;
    }

    private String qualifiedTable(String schema, String table) {
        if (schema != null && !schema.isBlank()) {
            return "\"" + schema.replace("\"", "\"\"") + "\".\"" + table.replace("\"", "\"\"") + "\"";
        }
        return "\"" + table.replace("\"", "\"\"") + "\"";
    }

    private Map<String, Object> toMap(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            row.put(meta.getColumnName(i), rs.getObject(i));
        }
        return row;
    }
}
