package com.nativelogix.rdbms2marklogic.service.generate;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.SavedConnection;
import com.nativelogix.rdbms2marklogic.model.generate.JsonPreviewResult;
import com.nativelogix.rdbms2marklogic.model.project.JsonDocumentModel;
import com.nativelogix.rdbms2marklogic.model.project.JsonTableMapping;
import com.nativelogix.rdbms2marklogic.model.project.NamingCase;
import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.repository.FileSystemProjectRepository;
import com.nativelogix.rdbms2marklogic.service.JDBCConnectionService;
import com.nativelogix.rdbms2marklogic.service.generate.JsonDocumentBuilder.MappedRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * Orchestrates JSON document generation for a project, parallel to {@link XmlGenerationService}.
 * Reuses {@link JoinResolver} and {@link SqlQueryBuilder} for RDBMS querying, and delegates
 * JSON assembly to {@link JsonDocumentBuilder}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JsonGenerationService {

    private final FileSystemProjectRepository projectRepository;
    private final JDBCConnectionService jdbcConnectionService;
    private final JoinResolver joinResolver;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;

    public JsonPreviewResult generatePreview(String projectId, int limit) {
        JsonPreviewResult result = new JsonPreviewResult();

        // 1. Load project
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));

        // 2. Validate mapping
        if (project.getMapping() == null || project.getMapping().getJsonDocumentModel() == null) {
            result.getErrors().add("Project has no JSON document model mapping defined.");
            return result;
        }
        JsonDocumentModel docModel = project.getMapping().getJsonDocumentModel();
        if (docModel.getRoot() == null) {
            result.getErrors().add("JSON document model has no root mapping.");
            return result;
        }

        // 3. Resolve connection
        String connectionName = project.getConnectionName();
        SavedConnection savedConn = jdbcConnectionService.getConnection(connectionName)
                .orElseThrow(() -> new IllegalArgumentException("Connection not found: " + connectionName));
        Connection conn = savedConn.getConnection();
        Connection.ConnectionType dbType = conn.getType() != null ? conn.getType() : Connection.ConnectionType.Postgres;

        NamingCase casing = project.getSettings() != null ? project.getSettings().getDefaultCasing() : null;

        JsonTableMapping rootMapping = docModel.getRoot();
        List<JsonTableMapping> allMappings = docModel.getElements() != null ? docModel.getElements() : List.of();

        // Build index: parentRef id → inline mappings (both InlineObject and Array types with parentRef)
        Map<String, List<JsonTableMapping>> inlinesByParentId = new LinkedHashMap<>();
        for (JsonTableMapping m : allMappings) {
            if (m.getParentRef() != null && ("InlineObject".equals(m.getMappingType()) || "Array".equals(m.getMappingType()))) {
                inlinesByParentId.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
            }
        }

        // Root-level mappings only (InlineObjects and InlineArrays are handled recursively)
        List<JsonTableMapping> rootLevelMappings = allMappings.stream()
                .filter(m -> m.getParentRef() == null)
                .toList();

        // 4. Query and build
        try (java.sql.Connection jdbc = jdbcConnectionService.openJdbcConnection(conn)) {

            String rootWhereClause = lookupWhereClause(project, rootMapping.getSourceSchema(), rootMapping.getSourceTable());
            String rootSql = sqlQueryBuilder.buildRootQuery(rootMapping, dbType, limit, rootWhereClause);
            log.debug("JSON root query: {}", rootSql);

            // Track null-FK skips per inline name to emit a single summary warning
            Map<String, int[]> nullFkCounts = new LinkedHashMap<>();

            try (PreparedStatement rootStmt = jdbc.prepareStatement(rootSql);
                 ResultSet rootRs = rootStmt.executeQuery()) {

                result.setTotalRows(0);

                while (rootRs.next()) {
                    result.setTotalRows(result.getTotalRows() + 1);
                    Map<String, Object> rootRow = toMap(rootRs);

                    Map<JsonTableMapping, List<MappedRow>> childData = new LinkedHashMap<>();

                    for (JsonTableMapping childMapping : rootLevelMappings) {
                        try {
                            JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, childMapping, project);

                            Object parentValue = rootRow.get(joinPath.parentColumn());
                            if (parentValue == null) {
                                nullFkCounts.computeIfAbsent(childMapping.getJsonName(), k -> new int[1])[0]++;
                                childData.put(childMapping, List.of());
                                continue;
                            }

                            List<MappedRow> mappedRows = queryMappedRows(
                                    jdbc, childMapping, joinPath.childColumn(), parentValue,
                                    inlinesByParentId, project, result);
                            childData.put(childMapping, mappedRows);

                        } catch (IllegalArgumentException e) {
                            log.warn("Join resolution failed for child '{}': {}", childMapping.getJsonName(), e.getMessage());
                            result.getErrors().add("Join not found for child table '%s.%s': %s"
                                    .formatted(childMapping.getSourceSchema(), childMapping.getSourceTable(), e.getMessage()));
                            childData.put(childMapping, List.of());
                        }
                    }

                    // InlineObject/Array mappings that are direct children of root (parentRef = rootMapping.id)
                    List<JsonTableMapping> rootDirectInlines =
                            inlinesByParentId.getOrDefault(rootMapping.getId(), List.of());
                    for (JsonTableMapping inlineMapping : rootDirectInlines) {
                        try {
                            JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, inlineMapping, project);

                            Object parentValue = rootRow.get(joinPath.parentColumn());
                            if (parentValue == null) {
                                nullFkCounts.computeIfAbsent(inlineMapping.getJsonName(), k -> new int[1])[0]++;
                                childData.put(inlineMapping, List.of());
                                continue;
                            }

                            List<MappedRow> mappedRows = queryMappedRows(
                                    jdbc, inlineMapping, joinPath.childColumn(), parentValue,
                                    inlinesByParentId, project, result);
                            childData.put(inlineMapping, mappedRows);

                        } catch (IllegalArgumentException e) {
                            log.warn("Join resolution failed for root inline '{}': {}", inlineMapping.getJsonName(), e.getMessage());
                            result.getErrors().add("Join not found for inline table '%s.%s': %s"
                                    .formatted(inlineMapping.getSourceSchema(), inlineMapping.getSourceTable(), e.getMessage()));
                            childData.put(inlineMapping, List.of());
                        }
                    }

                    try {
                        String json = jsonDocumentBuilder.build(rootMapping, rootRow, childData, casing);
                        result.getDocuments().add(json);
                    } catch (Exception e) {
                        log.error("JSON build failed for row {}: {}", result.getTotalRows(), e.getMessage(), e);
                        result.getErrors().add("JSON build error on row %d: %s".formatted(result.getTotalRows(), e.getMessage()));
                    }
                }
            }

            nullFkCounts.forEach((name, count) ->
                result.getErrors().add("'%s' was null for %d row%s — rendered as null in output"
                        .formatted(name, count[0], count[0] == 1 ? "" : "s")));

        } catch (Exception e) {
            log.error("JSON preview generation failed for project {}: {}", projectId, e.getMessage(), e);
            result.getErrors().add("Generation failed: " + e.getMessage());
        }

        return result;
    }

    private List<MappedRow> queryMappedRows(java.sql.Connection jdbc,
                                            JsonTableMapping mapping,
                                            String childJoinCol,
                                            Object joinValue,
                                            Map<String, List<JsonTableMapping>> inlinesByParentId,
                                            Project project,
                                            JsonPreviewResult result) throws Exception {

        String sql = sqlQueryBuilder.buildChildQuery(mapping, childJoinCol);
        log.debug("JSON child query for '{}': {}", mapping.getJsonName(), sql);

        List<MappedRow> mappedRows = new ArrayList<>();

        try (PreparedStatement stmt = jdbc.prepareStatement(sql)) {
            stmt.setObject(1, joinValue);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = toMap(rs);
                    Map<JsonTableMapping, List<MappedRow>> inlineChildren =
                            queryInlineChildren(jdbc, mapping, row, inlinesByParentId, project, result);
                    mappedRows.add(new MappedRow(row, inlineChildren));
                }
            }
        }

        return mappedRows;
    }

    private Map<JsonTableMapping, List<MappedRow>> queryInlineChildren(
            java.sql.Connection jdbc,
            JsonTableMapping parentMapping,
            Map<String, Object> parentRow,
            Map<String, List<JsonTableMapping>> inlinesByParentId,
            Project project,
            JsonPreviewResult result) throws Exception {

        List<JsonTableMapping> inlines = inlinesByParentId.get(parentMapping.getId());
        if (inlines == null || inlines.isEmpty()) return Map.of();

        Map<JsonTableMapping, List<MappedRow>> inlineData = new LinkedHashMap<>();

        for (JsonTableMapping inline : inlines) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);

                Object joinValue = parentRow.get(joinPath.parentColumn());
                if (joinValue == null) {
                    log.warn("Parent join column '{}' is null — skipping inline '{}'",
                            joinPath.parentColumn(), inline.getJsonName());
                    inlineData.put(inline, List.of());
                    continue;
                }

                List<MappedRow> rows = queryMappedRows(
                        jdbc, inline, joinPath.childColumn(), joinValue,
                        inlinesByParentId, project, result);
                inlineData.put(inline, rows);

            } catch (IllegalArgumentException e) {
                log.warn("Join resolution failed for inline '{}': {}", inline.getJsonName(), e.getMessage());
                result.getErrors().add("Join not found for inline table '%s.%s': %s"
                        .formatted(inline.getSourceSchema(), inline.getSourceTable(), e.getMessage()));
                inlineData.put(inline, List.of());
            }
        }

        return inlineData;
    }

    private Map<String, Object> toMap(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            row.put(meta.getColumnName(i), rs.getObject(i));
        }
        return row;
    }

    /** Looks up the WHERE clause for a table from the project's schema metadata. */
    private String lookupWhereClause(Project project, String schema, String table) {
        if (project.getSchemas() == null) return null;
        var dbSchema = project.getSchemas().get(schema);
        if (dbSchema == null || dbSchema.getTables() == null) return null;
        var dbTable = dbSchema.getTables().get(table);
        return dbTable != null ? dbTable.getWhereClause() : null;
    }
}
