package com.nativelogix.rdbms2marklogic.service.generate;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.SavedConnection;
import com.nativelogix.rdbms2marklogic.model.generate.XmlPreviewResult;
import com.nativelogix.rdbms2marklogic.model.project.DocumentModel;
import com.nativelogix.rdbms2marklogic.model.project.NamingCase;
import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.model.project.XmlNamespace;
import com.nativelogix.rdbms2marklogic.model.project.XmlTableMapping;
import com.nativelogix.rdbms2marklogic.repository.FileSystemProjectRepository;
import com.nativelogix.rdbms2marklogic.service.JDBCConnectionService;
import com.nativelogix.rdbms2marklogic.service.generate.XmlDocumentBuilder.MappedRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * Orchestrates XML document generation for a project:
 * <ol>
 *   <li>Loads the project and its RDBMS connection.</li>
 *   <li>Queries the root table (up to {@code limit} rows).</li>
 *   <li>For each root row, queries direct child (Elements) mappings via their join path.</li>
 *   <li>For each child row, recursively queries any InlineElement mappings whose
 *       {@code parentRef} matches the child's mapping id.</li>
 *   <li>Delegates XML assembly to {@link XmlDocumentBuilder}.</li>
 * </ol>
 *
 * <p>InlineElement mappings are never resolved against the root row directly.
 * Instead they are nested inside each row of whichever mapping their {@code parentRef}
 * points to, at any depth.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class XmlGenerationService {

    private final FileSystemProjectRepository projectRepository;
    private final JDBCConnectionService jdbcConnectionService;
    private final JoinResolver joinResolver;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final XmlDocumentBuilder xmlDocumentBuilder;

    /**
     * Generate a preview of up to {@code limit} XML documents for the given project.
     */
    public XmlPreviewResult generatePreview(String projectId, int limit) {
        XmlPreviewResult result = new XmlPreviewResult();

        // 1. Load project
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));

        // 2. Validate mapping
        if (project.getMapping() == null || project.getMapping().getDocumentModel() == null) {
            result.getErrors().add("Project has no document model mapping defined.");
            return result;
        }
        DocumentModel docModel = project.getMapping().getDocumentModel();
        if (docModel.getRoot() == null) {
            result.getErrors().add("Document model has no root table mapping.");
            return result;
        }

        // 3. Resolve connection
        String connectionName = project.getConnectionName();
        SavedConnection savedConn = jdbcConnectionService.getConnection(connectionName)
                .orElseThrow(() -> new IllegalArgumentException("Connection not found: " + connectionName));
        Connection conn = savedConn.getConnection();
        Connection.ConnectionType dbType = conn.getType() != null ? conn.getType() : Connection.ConnectionType.Postgres;

        NamingCase casing = project.getSettings() != null ? project.getSettings().getDefaultCasing() : null;
        List<XmlNamespace> namespaces = project.getMapping().getNamespaces();

        XmlTableMapping rootMapping = docModel.getRoot();
        List<XmlTableMapping> allMappings = docModel.getElements() != null ? docModel.getElements() : List.of();

        // Build index: parentRef id → InlineElement mappings that belong to it
        Map<String, List<XmlTableMapping>> inlinesByParentId = new LinkedHashMap<>();
        for (XmlTableMapping m : allMappings) {
            if ("InlineElement".equals(m.getMappingType()) && m.getParentRef() != null) {
                inlinesByParentId.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
            }
        }

        // Root-level mappings: Elements type only (InlineElements are handled recursively)
        List<XmlTableMapping> rootLevelMappings = allMappings.stream()
                .filter(m -> !"InlineElement".equals(m.getMappingType()))
                .toList();

        // 4. Query and build
        try (java.sql.Connection jdbc = jdbcConnectionService.openJdbcConnection(conn)) {

            String rootSql = sqlQueryBuilder.buildRootQuery(rootMapping, dbType, limit);
            log.debug("Root query: {}", rootSql);

            try (PreparedStatement rootStmt = jdbc.prepareStatement(rootSql);
                 ResultSet rootRs = rootStmt.executeQuery()) {

                result.setTotalRows(0);

                while (rootRs.next()) {
                    result.setTotalRows(result.getTotalRows() + 1);
                    Map<String, Object> rootRow = toMap(rootRs);

                    // Build child data: each Elements mapping → its MappedRows (with inline children)
                    Map<XmlTableMapping, List<MappedRow>> childData = new LinkedHashMap<>();

                    for (XmlTableMapping childMapping : rootLevelMappings) {
                        try {
                            JoinResolver.JoinPath joinPath = joinResolver.resolve(rootMapping, childMapping, project);

                            Object parentValue = rootRow.get(joinPath.parentColumn());
                            if (parentValue == null) {
                                log.warn("Parent join column '{}' is null in root row — skipping child '{}'",
                                        joinPath.parentColumn(), childMapping.getXmlName());
                                childData.put(childMapping, List.of());
                                continue;
                            }

                            List<MappedRow> mappedRows = queryMappedRows(
                                    jdbc, childMapping, joinPath.childColumn(), parentValue,
                                    inlinesByParentId, project, result);
                            childData.put(childMapping, mappedRows);

                        } catch (IllegalArgumentException e) {
                            log.warn("Join resolution failed for child '{}': {}", childMapping.getXmlName(), e.getMessage());
                            result.getErrors().add("Join not found for child table '%s.%s': %s"
                                    .formatted(childMapping.getSourceSchema(), childMapping.getSourceTable(), e.getMessage()));
                            childData.put(childMapping, List.of());
                        }
                    }

                    // Build XML document
                    try {
                        String xml = xmlDocumentBuilder.build(rootMapping, rootRow, childData, casing, namespaces);
                        result.getDocuments().add(xml);
                    } catch (Exception e) {
                        log.error("XML build failed for row {}: {}", result.getTotalRows(), e.getMessage(), e);
                        result.getErrors().add("XML build error on row %d: %s".formatted(result.getTotalRows(), e.getMessage()));
                    }
                }
            }

        } catch (Exception e) {
            log.error("Preview generation failed for project {}: {}", projectId, e.getMessage(), e);
            result.getErrors().add("Generation failed: " + e.getMessage());
        }

        return result;
    }

    /**
     * Queries rows for {@code mapping} filtered by {@code joinValue}, then recursively
     * queries any InlineElement children for each returned row.
     */
    private List<MappedRow> queryMappedRows(java.sql.Connection jdbc,
                                            XmlTableMapping mapping,
                                            String childJoinCol,
                                            Object joinValue,
                                            Map<String, List<XmlTableMapping>> inlinesByParentId,
                                            Project project,
                                            XmlPreviewResult result) throws Exception {

        String sql = sqlQueryBuilder.buildChildQuery(mapping, childJoinCol);
        log.debug("Child query for '{}': {}", mapping.getXmlName(), sql);

        List<MappedRow> mappedRows = new ArrayList<>();

        try (PreparedStatement stmt = jdbc.prepareStatement(sql)) {
            stmt.setObject(1, joinValue);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = toMap(rs);
                    Map<XmlTableMapping, List<MappedRow>> inlineChildren =
                            queryInlineChildren(jdbc, mapping, row, inlinesByParentId, project, result);
                    mappedRows.add(new MappedRow(row, inlineChildren));
                }
            }
        }

        return mappedRows;
    }

    /**
     * Recursively queries InlineElement children for one parent row.
     *
     * @param parentMapping  the mapping whose id is used as the parentRef lookup key
     * @param parentRow      the data row of the parent — used to extract the join column value
     */
    private Map<XmlTableMapping, List<MappedRow>> queryInlineChildren(
            java.sql.Connection jdbc,
            XmlTableMapping parentMapping,
            Map<String, Object> parentRow,
            Map<String, List<XmlTableMapping>> inlinesByParentId,
            Project project,
            XmlPreviewResult result) throws Exception {

        List<XmlTableMapping> inlines = inlinesByParentId.get(parentMapping.getId());
        if (inlines == null || inlines.isEmpty()) return Map.of();

        Map<XmlTableMapping, List<MappedRow>> inlineData = new LinkedHashMap<>();

        for (XmlTableMapping inline : inlines) {
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(parentMapping, inline, project);

                Object joinValue = parentRow.get(joinPath.parentColumn());
                if (joinValue == null) {
                    log.warn("Parent join column '{}' is null — skipping inline '{}'",
                            joinPath.parentColumn(), inline.getXmlName());
                    inlineData.put(inline, List.of());
                    continue;
                }

                List<MappedRow> rows = queryMappedRows(
                        jdbc, inline, joinPath.childColumn(), joinValue,
                        inlinesByParentId, project, result);
                inlineData.put(inline, rows);

            } catch (IllegalArgumentException e) {
                log.warn("Join resolution failed for inline '{}': {}", inline.getXmlName(), e.getMessage());
                result.getErrors().add("Join not found for inline table '%s.%s': %s"
                        .formatted(inline.getSourceSchema(), inline.getSourceTable(), e.getMessage()));
                inlineData.put(inline, List.of());
            }
        }

        return inlineData;
    }

    /** Converts the current ResultSet row to a column-name → value map. */
    private Map<String, Object> toMap(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            row.put(meta.getColumnName(i), rs.getObject(i));
        }
        return row;
    }
}
