package com.nativelogix.rdbms2marklogic.service.migration;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.project.*;
import com.nativelogix.rdbms2marklogic.service.JDBCConnectionService;
import com.nativelogix.rdbms2marklogic.service.generate.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Spring Batch ItemReader that streams root rows from the RDBMS using a JDBC cursor,
 * fetches child rows for each, and builds the full document string.
 *
 * <h3>Performance optimisations</h3>
 * <ul>
 *   <li><b>Lookup cache</b> — child/inline tables with fewer than {@value #LOOKUP_CACHE_THRESHOLD}
 *       rows are loaded entirely into memory once during {@link #open} so that all subsequent
 *       lookups are O(1) in-process instead of individual JDBC round trips.</li>
 *   <li><b>Prefetch + batch IN queries</b> — root rows are read in batches of
 *       {@value #PREFETCH_SIZE}.  For each non-cached level-1 child table a single
 *       {@code WHERE fk IN (…)} query replaces N individual queries.</li>
 *   <li><b>Partition support</b> — when {@code partitionOffset/partitionPageSize} are set
 *       (by {@link MigrationJobService} parallel flow), the root query uses
 *       {@code ORDER BY 1 LIMIT/OFFSET} so multiple reader instances can process
 *       disjoint slices of the root table concurrently.</li>
 * </ul>
 */
public class RdbmsDocumentReader implements ItemStreamReader<DocumentBuildResult> {

    private static final Logger log = LoggerFactory.getLogger(RdbmsDocumentReader.class);

    /** Child tables with at most this many rows are loaded entirely into memory. */
    static final int LOOKUP_CACHE_THRESHOLD = 10_000;
    /** Root rows consumed per prefetch batch. */
    static final int PREFETCH_SIZE = 2000;

    private final MigrationJobContext ctx;
    private final JDBCConnectionService jdbcConnectionService;
    private final SqlQueryBuilder sqlQueryBuilder;
    private final JoinResolver joinResolver;
    private final XmlDocumentBuilder xmlDocumentBuilder;
    private final JsonDocumentBuilder jsonDocumentBuilder;

    /** 0 = no offset (full table scan or single partition). */
    private final long partitionOffset;
    /** -1 = no limit (full table scan). */
    private final long partitionPageSize;

    private java.sql.Connection jdbcConn;
    private ResultSet rootRs;
    private PreparedStatement rootStmt;

    private int rowIndex = 0;
    private String mappingType;
    private Connection.ConnectionType dbType;

    // ── XML mapping state ─────────────────────────────────────────────────────
    private XmlTableMapping xmlRootMapping;
    private List<XmlTableMapping> xmlRootLevelMappings;
    private Map<String, List<XmlTableMapping>> xmlInlinesByParentId;

    // ── JSON mapping state ────────────────────────────────────────────────────
    private JsonTableMapping jsonRootMapping;
    private List<JsonTableMapping> jsonRootLevelMappings;
    private Map<String, List<JsonTableMapping>> jsonInlinesByParentId;

    // ── Lookup cache ──────────────────────────────────────────────────────────
    /**
     * Raw rows for each cached mapping, indexed by join-column value.
     * Structure: mappingId → ("childColName:value" → rows).
     * The index key is built lazily on first use via {@link #lookupFromCache}.
     */
    private final Map<String, List<Map<String, Object>>> lookupCacheRows  = new HashMap<>();
    /**
     * Lazily-built per-column indices: "mappingId:colName" → (colValue → rows).
     */
    private final Map<String, Map<String, List<Map<String, Object>>>> lookupCacheIndex = new HashMap<>();
    private final Set<String> cachedMappingIds = new HashSet<>();

    // ── Prefetch queue ────────────────────────────────────────────────────────
    private final Deque<DocumentBuildResult> prefetchQueue = new ArrayDeque<>(PREFETCH_SIZE);
    private boolean rootExhausted = false;

    // ── Constructors ──────────────────────────────────────────────────────────

    /** Non-partitioned: full table scan. */
    public RdbmsDocumentReader(MigrationJobContext ctx,
                                JDBCConnectionService jdbcConnectionService,
                                SqlQueryBuilder sqlQueryBuilder,
                                JoinResolver joinResolver,
                                XmlDocumentBuilder xmlDocumentBuilder,
                                JsonDocumentBuilder jsonDocumentBuilder) {
        this(ctx, 0L, -1L,
             jdbcConnectionService, sqlQueryBuilder, joinResolver,
             xmlDocumentBuilder, jsonDocumentBuilder);
    }

    /** Partitioned: reads only the slice [offset, offset + pageSize). */
    public RdbmsDocumentReader(MigrationJobContext ctx,
                                long partitionOffset,
                                long partitionPageSize,
                                JDBCConnectionService jdbcConnectionService,
                                SqlQueryBuilder sqlQueryBuilder,
                                JoinResolver joinResolver,
                                XmlDocumentBuilder xmlDocumentBuilder,
                                JsonDocumentBuilder jsonDocumentBuilder) {
        this.ctx                   = ctx;
        this.partitionOffset       = partitionOffset;
        this.partitionPageSize     = partitionPageSize;
        this.jdbcConnectionService = jdbcConnectionService;
        this.sqlQueryBuilder       = sqlQueryBuilder;
        this.joinResolver          = joinResolver;
        this.xmlDocumentBuilder    = xmlDocumentBuilder;
        this.jsonDocumentBuilder   = jsonDocumentBuilder;
    }

    // ── ItemStream ────────────────────────────────────────────────────────────

    @Override
    public void open(@org.springframework.lang.NonNull ExecutionContext executionContext) {
        Project project = ctx.getProject();
        mappingType = project.getMapping() != null ? project.getMapping().getMappingType() : "XML";
        if (mappingType == null) mappingType = "XML";

        Connection conn = ctx.getSourceConnection().getConnection();
        dbType = conn.getType() != null ? conn.getType() : Connection.ConnectionType.Postgres;

        try {
            jdbcConn = jdbcConnectionService.openJdbcConnection(conn);

            if ("JSON".equalsIgnoreCase(mappingType)) {
                JsonDocumentModel docModel = project.getMapping().getJsonDocumentModel();
                jsonRootMapping = docModel.getRoot();
                List<JsonTableMapping> allMappings =
                        docModel.getElements() != null ? docModel.getElements() : List.of();
                jsonInlinesByParentId = new LinkedHashMap<>();
                for (JsonTableMapping m : allMappings) {
                    if (m.getParentRef() != null
                            && ("InlineObject".equals(m.getMappingType()) || "Array".equals(m.getMappingType()))) {
                        jsonInlinesByParentId.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
                    }
                }
                jsonRootLevelMappings = allMappings.stream().filter(m -> m.getParentRef() == null).toList();

                loadLookupCachesJson(allMappings);

                String sql = buildRootSql(jsonRootMapping);
                rootStmt = jdbcConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                rootStmt.setFetchSize(PREFETCH_SIZE);

            } else {
                DocumentModel docModel = project.getMapping().getDocumentModel();
                xmlRootMapping = docModel.getRoot();
                List<XmlTableMapping> allMappings =
                        docModel.getElements() != null ? docModel.getElements() : List.of();
                xmlInlinesByParentId = new LinkedHashMap<>();
                for (XmlTableMapping m : allMappings) {
                    if (m.getParentRef() != null) {
                        xmlInlinesByParentId.computeIfAbsent(m.getParentRef(), k -> new ArrayList<>()).add(m);
                    }
                }
                xmlRootLevelMappings = allMappings.stream().filter(m -> m.getParentRef() == null).toList();

                loadLookupCachesXml(allMappings);

                String sql = buildRootSql(xmlRootMapping);
                rootStmt = jdbcConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                rootStmt.setFetchSize(PREFETCH_SIZE);
            }

            rootRs = rootStmt.executeQuery();

        } catch (Exception e) {
            throw new RuntimeException("Failed to open RDBMS cursor: " + e.getMessage(), e);
        }
    }

    @Override
    public DocumentBuildResult read() throws Exception {
        if (!prefetchQueue.isEmpty()) return prefetchQueue.poll();
        if (rootExhausted) return null;
        fillPrefetchQueue();
        return prefetchQueue.poll();
    }

    @Override
    public void close() {
        try { if (rootRs   != null) rootRs.close();   } catch (Exception ignored) {}
        try { if (rootStmt != null) rootStmt.close(); } catch (Exception ignored) {}
        try { if (jdbcConn != null) jdbcConn.close(); } catch (Exception ignored) {}
    }

    // ── Prefetch & batch child loading ────────────────────────────────────────

    /**
     * Reads up to {@value #PREFETCH_SIZE} root rows, batch-fetches all non-cached level-1
     * child data with IN queries, then builds documents and enqueues them.
     */
    private void fillPrefetchQueue() throws Exception {
        List<Map<String, Object>> rootBatch = new ArrayList<>(PREFETCH_SIZE);
        while (rootBatch.size() < PREFETCH_SIZE && rootRs.next()) {
            rowIndex++;
            rootBatch.add(toMap(rootRs));
        }
        if (rootBatch.isEmpty()) {
            rootExhausted = true;
            return;
        }

        Project project = ctx.getProject();
        NamingCase casing = project.getSettings() != null ? project.getSettings().getDefaultCasing() : null;

        if ("JSON".equalsIgnoreCase(mappingType)) {
            Map<String, Map<String, List<Map<String, Object>>>> batchData =
                    batchFetchJsonChildren(jsonRootLevelMappings, rootBatch);

            for (int i = 0; i < rootBatch.size(); i++) {
                Map<String, Object> rootRow = rootBatch.get(i);
                int globalIdx = rowIndex - rootBatch.size() + i + 1;
                String uri = buildUri(rootRow, globalIdx);

                Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
                for (JsonTableMapping child : jsonRootLevelMappings) {
                    try {
                        JoinResolver.JoinPath joinPath = joinResolver.resolve(jsonRootMapping, child, project);
                        Object parentVal = rootRow.get(joinPath.parentColumn());
                        if (parentVal == null) { childData.put(child, List.of()); continue; }
                        childData.put(child, resolveJsonChildRows(child, joinPath.childColumn(), parentVal, project, batchData));
                    } catch (Exception e) {
                        log.warn("Child join failed for '{}': {}", child.getJsonName(), e.getMessage());
                        childData.put(child, List.of());
                    }
                }
                String content = jsonDocumentBuilder.build(jsonRootMapping, rootRow, childData, casing);
                prefetchQueue.add(new DocumentBuildResult(uri + ".json", content, "JSON"));
            }

        } else {
            Map<String, Map<String, List<Map<String, Object>>>> batchData =
                    batchFetchXmlChildren(xmlRootLevelMappings, rootBatch);

            List<XmlNamespace> namespaces = project.getMapping().getNamespaces();
            for (int i = 0; i < rootBatch.size(); i++) {
                Map<String, Object> rootRow = rootBatch.get(i);
                int globalIdx = rowIndex - rootBatch.size() + i + 1;
                String uri = buildUri(rootRow, globalIdx);

                Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
                for (XmlTableMapping child : xmlRootLevelMappings) {
                    try {
                        JoinResolver.JoinPath joinPath = joinResolver.resolve(xmlRootMapping, child, project);
                        Object parentVal = rootRow.get(joinPath.parentColumn());
                        if (parentVal == null) { childData.put(child, List.of()); continue; }
                        childData.put(child, resolveXmlChildRows(child, joinPath.childColumn(), parentVal, project, batchData));
                    } catch (Exception e) {
                        log.warn("Child join failed for '{}': {}", child.getXmlName(), e.getMessage());
                        childData.put(child, List.of());
                    }
                }
                String content = xmlDocumentBuilder.build(xmlRootMapping, rootRow, childData, casing, namespaces);
                prefetchQueue.add(new DocumentBuildResult(uri + ".xml", content, "XML"));
            }
        }
    }

    /**
     * For each non-cached XML child mapping, fetches all matching child rows for the entire
     * root batch using a single {@code WHERE fk IN (…)} query.
     *
     * @return mappingId → (fkValue → rows)
     */
    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchXmlChildren(
            List<XmlTableMapping> children, List<Map<String, Object>> rootBatch) {
        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        Project project = ctx.getProject();
        for (XmlTableMapping child : children) {
            if (cachedMappingIds.contains(child.getId())) continue;
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(xmlRootMapping, child, project);
                List<Object> parentValues = rootBatch.stream()
                        .map(row -> row.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (parentValues.isEmpty()) continue;

                String sql = sqlQueryBuilder.buildChildBatchQuery(child, joinPath.childColumn(), parentValues.size());
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < parentValues.size(); i++) stmt.setObject(i + 1, parentValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs);
                            String key = String.valueOf(row.get(joinPath.childColumn()));
                            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                result.put(child.getId(), grouped);
            } catch (Exception e) {
                log.warn("Batch child fetch failed for '{}': {}", child.getXmlName(), e.getMessage());
                result.put(child.getId(), Map.of());
            }
        }
        return result;
    }

    private Map<String, Map<String, List<Map<String, Object>>>> batchFetchJsonChildren(
            List<JsonTableMapping> children, List<Map<String, Object>> rootBatch) {
        Map<String, Map<String, List<Map<String, Object>>>> result = new HashMap<>();
        Project project = ctx.getProject();
        for (JsonTableMapping child : children) {
            if (cachedMappingIds.contains(child.getId())) continue;
            try {
                JoinResolver.JoinPath joinPath = joinResolver.resolve(jsonRootMapping, child, project);
                List<Object> parentValues = rootBatch.stream()
                        .map(row -> row.get(joinPath.parentColumn()))
                        .filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (parentValues.isEmpty()) continue;

                String sql = sqlQueryBuilder.buildChildBatchQuery(child, joinPath.childColumn(), parentValues.size());
                Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
                try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
                    for (int i = 0; i < parentValues.size(); i++) stmt.setObject(i + 1, parentValues.get(i));
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            Map<String, Object> row = toMap(rs);
                            String key = String.valueOf(row.get(joinPath.childColumn()));
                            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                        }
                    }
                }
                result.put(child.getId(), grouped);
            } catch (Exception e) {
                log.warn("Batch child fetch failed for '{}': {}", child.getJsonName(), e.getMessage());
                result.put(child.getId(), Map.of());
            }
        }
        return result;
    }

    // ── Row resolution: cache → batch data → per-row fallback ────────────────

    private List<XmlDocumentBuilder.MappedRow> resolveXmlChildRows(
            XmlTableMapping mapping, String childCol, Object parentValue, Project project,
            Map<String, Map<String, List<Map<String, Object>>>> batchData) throws Exception {
        String key = String.valueOf(parentValue);

        if (cachedMappingIds.contains(mapping.getId())) {
            return toXmlMappedRows(mapping, lookupFromCache(mapping.getId(), childCol, key), project);
        }
        if (batchData.containsKey(mapping.getId())) {
            return toXmlMappedRows(mapping, batchData.get(mapping.getId()).getOrDefault(key, List.of()), project);
        }
        return queryXmlMappedRows(mapping, childCol, parentValue, project);
    }

    private List<JsonDocumentBuilder.MappedRow> resolveJsonChildRows(
            JsonTableMapping mapping, String childCol, Object parentValue, Project project,
            Map<String, Map<String, List<Map<String, Object>>>> batchData) throws Exception {
        String key = String.valueOf(parentValue);

        if (cachedMappingIds.contains(mapping.getId())) {
            return toJsonMappedRows(mapping, lookupFromCache(mapping.getId(), childCol, key), project);
        }
        if (batchData.containsKey(mapping.getId())) {
            return toJsonMappedRows(mapping, batchData.get(mapping.getId()).getOrDefault(key, List.of()), project);
        }
        return queryJsonMappedRows(mapping, childCol, parentValue, project);
    }

    private List<XmlDocumentBuilder.MappedRow> toXmlMappedRows(
            XmlTableMapping mapping, List<Map<String, Object>> rows, Project project) throws Exception {
        List<XmlDocumentBuilder.MappedRow> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            result.add(new XmlDocumentBuilder.MappedRow(row, queryXmlInlineChildren(mapping, row, project)));
        }
        return result;
    }

    private List<JsonDocumentBuilder.MappedRow> toJsonMappedRows(
            JsonTableMapping mapping, List<Map<String, Object>> rows, Project project) throws Exception {
        List<JsonDocumentBuilder.MappedRow> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            result.add(new JsonDocumentBuilder.MappedRow(row, queryJsonInlineChildren(mapping, row, project)));
        }
        return result;
    }

    // ── Per-row query methods (inline children / fallback) ────────────────────

    private List<XmlDocumentBuilder.MappedRow> queryXmlMappedRows(
            XmlTableMapping mapping, String childCol, Object joinValue, Project project) throws Exception {
        String sql = sqlQueryBuilder.buildChildQuery(mapping, childCol);
        List<XmlDocumentBuilder.MappedRow> result = new ArrayList<>();
        try (PreparedStatement stmt = jdbcConn.prepareStatement(sql)) {
            stmt.setObject(1, joinValue);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = toMap(rs);
                    result.add(new XmlDocumentBuilder.MappedRow(row, queryXmlInlineChildren(mapping, row, project)));
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
                if (cachedMappingIds.contains(inline.getId())) {
                    result.put(inline, toXmlMappedRows(inline,
                            lookupFromCache(inline.getId(), joinPath.childColumn(), String.valueOf(joinValue)), project));
                } else {
                    result.put(inline, queryXmlMappedRows(inline, joinPath.childColumn(), joinValue, project));
                }
            } catch (Exception e) {
                result.put(inline, List.of());
            }
        }
        return result;
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
                    result.add(new JsonDocumentBuilder.MappedRow(row, queryJsonInlineChildren(mapping, row, project)));
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
                if (cachedMappingIds.contains(inline.getId())) {
                    result.put(inline, toJsonMappedRows(inline,
                            lookupFromCache(inline.getId(), joinPath.childColumn(), String.valueOf(joinValue)), project));
                } else {
                    result.put(inline, queryJsonMappedRows(inline, joinPath.childColumn(), joinValue, project));
                }
            } catch (Exception e) {
                result.put(inline, List.of());
            }
        }
        return result;
    }

    // ── Lookup cache loading ──────────────────────────────────────────────────

    private void loadLookupCachesXml(List<XmlTableMapping> allMappings) {
        for (XmlTableMapping mapping : allMappings) {
            if (cachedMappingIds.contains(mapping.getId())) continue;
            try {
                long count = countTableRows(mapping.getSourceSchema(), mapping.getSourceTable());
                if (count > 0 && count <= LOOKUP_CACHE_THRESHOLD) {
                    List<Map<String, Object>> rows = loadAllRows(mapping.getSourceSchema(), mapping.getSourceTable());
                    lookupCacheRows.put(mapping.getId(), rows);
                    cachedMappingIds.add(mapping.getId());
                    log.info("Cached lookup table '{}' ({} rows)", mapping.getSourceTable(), rows.size());
                }
            } catch (Exception e) {
                log.debug("Skipping lookup cache for '{}': {}", mapping.getSourceTable(), e.getMessage());
            }
        }
    }

    private void loadLookupCachesJson(List<JsonTableMapping> allMappings) {
        for (JsonTableMapping mapping : allMappings) {
            if (cachedMappingIds.contains(mapping.getId())) continue;
            try {
                long count = countTableRows(mapping.getSourceSchema(), mapping.getSourceTable());
                if (count > 0 && count <= LOOKUP_CACHE_THRESHOLD) {
                    List<Map<String, Object>> rows = loadAllRows(mapping.getSourceSchema(), mapping.getSourceTable());
                    lookupCacheRows.put(mapping.getId(), rows);
                    cachedMappingIds.add(mapping.getId());
                    log.info("Cached lookup table '{}' ({} rows)", mapping.getSourceTable(), rows.size());
                }
            } catch (Exception e) {
                log.debug("Skipping lookup cache for '{}': {}", mapping.getSourceTable(), e.getMessage());
            }
        }
    }

    /**
     * Returns rows from the in-memory cache filtered by {@code colName = colValue}.
     * The per-column index is built lazily on first use and reused for all subsequent calls.
     */
    private List<Map<String, Object>> lookupFromCache(String mappingId, String colName, String colValue) {
        String indexKey = mappingId + ":" + colName;
        if (!lookupCacheIndex.containsKey(indexKey)) {
            List<Map<String, Object>> allRows = lookupCacheRows.getOrDefault(mappingId, List.of());
            Map<String, List<Map<String, Object>>> index = new HashMap<>();
            for (Map<String, Object> row : allRows) {
                Object val = row.get(colName);
                if (val != null) {
                    index.computeIfAbsent(String.valueOf(val), k -> new ArrayList<>()).add(row);
                }
            }
            lookupCacheIndex.put(indexKey, index);
        }
        return lookupCacheIndex.get(indexKey).getOrDefault(colValue, List.of());
    }

    private long countTableRows(String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTable(schema, table);
        try (PreparedStatement stmt = jdbcConn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private List<Map<String, Object>> loadAllRows(String schema, String table) throws SQLException {
        String sql = "SELECT * FROM " + qualifiedTable(schema, table);
        List<Map<String, Object>> rows = new ArrayList<>();
        try (PreparedStatement stmt = jdbcConn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) rows.add(toMap(rs));
        }
        return rows;
    }

    // ── SQL helpers ───────────────────────────────────────────────────────────

    private String buildRootSql(XmlTableMapping mapping) {
        String whereClause = lookupWhereClause(mapping.getSourceSchema(), mapping.getSourceTable());
        if (partitionPageSize > 0) {
            return sqlQueryBuilder.buildPagedRootQuery(mapping, dbType, whereClause, partitionOffset, partitionPageSize);
        }
        String cols  = buildXmlColList(mapping);
        String table = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        String sql   = "SELECT " + cols + " FROM " + table;
        if (whereClause != null && !whereClause.isBlank()) sql += " WHERE " + whereClause;
        return sql;
    }

    private String buildRootSql(JsonTableMapping mapping) {
        String whereClause = lookupWhereClause(mapping.getSourceSchema(), mapping.getSourceTable());
        if (partitionPageSize > 0) {
            return sqlQueryBuilder.buildPagedRootQuery(mapping, dbType, whereClause, partitionOffset, partitionPageSize);
        }
        String cols  = buildJsonColList(mapping);
        String table = qualifiedTable(mapping.getSourceSchema(), mapping.getSourceTable());
        String sql   = "SELECT " + cols + " FROM " + table;
        if (whereClause != null && !whereClause.isBlank()) sql += " WHERE " + whereClause;
        return sql;
    }

    private String buildUri(Map<String, Object> rootRow, int idx) {
        String dirPath = ctx.getDirectoryPath();
        if (dirPath == null || dirPath.isBlank()) dirPath = "/";
        if (!dirPath.endsWith("/")) dirPath += "/";

        String rootElement = "JSON".equalsIgnoreCase(mappingType)
                ? (jsonRootMapping != null ? jsonRootMapping.getJsonName() : "doc")
                : (xmlRootMapping  != null ? xmlRootMapping.getXmlName()   : "doc");

        String dir = dirPath.replace("{rootElement}", rootElement).replace("{index}", String.valueOf(idx));
        String pkValue = findPkValue(rootRow);
        return dir + (pkValue != null ? pkValue : String.valueOf(idx));
    }

    private String findPkValue(Map<String, Object> row) {
        for (String key : row.keySet()) {
            String lower = key.toLowerCase();
            if (lower.equals("id") || lower.endsWith("_id") || lower.endsWith("id")) {
                Object val = row.get(key);
                if (val != null) return val.toString();
            }
        }
        return null;
    }

    private String lookupWhereClause(String schema, String table) {
        var schemas = ctx.getProject().getSchemas();
        if (schemas == null) return null;
        var dbSchema = schemas.get(schema);
        if (dbSchema == null || dbSchema.getTables() == null) return null;
        var dbTable = dbSchema.getTables().get(table);
        return dbTable != null ? dbTable.getWhereClause() : null;
    }

    private String qualifiedTable(String schema, String table) {
        if (schema != null && !schema.isBlank()) {
            // SchemaCrawler returns SQL Server schema names as "database.schema" (e.g. "AdventureWorks2022.HumanResources").
            // Split on dots and quote each part to produce valid multi-part identifiers.
            String quotedSchema = java.util.Arrays.stream(schema.split("\\."))
                    .map(p -> "\"" + p.replace("\"", "\"\"") + "\"")
                    .collect(java.util.stream.Collectors.joining("."));
            return quotedSchema + ".\"" + table.replace("\"", "\"\"") + "\"";
        }
        return "\"" + table.replace("\"", "\"\"") + "\"";
    }

    private String buildXmlColList(XmlTableMapping mapping) {
        if (mapping.getColumns() == null || mapping.getColumns().isEmpty()) return "*";
        return mapping.getColumns().stream()
                .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                .map(c -> "\"" + c.getSourceColumn().replace("\"", "\"\"") + "\"")
                .reduce((a, b) -> a + ", " + b).orElse("*");
    }

    private String buildJsonColList(JsonTableMapping mapping) {
        if (mapping.getColumns() == null || mapping.getColumns().isEmpty()) return "*";
        return mapping.getColumns().stream()
                .filter(c -> !"CUSTOM".equals(c.getMappingType()))
                .map(c -> "\"" + c.getSourceColumn().replace("\"", "\"\"") + "\"")
                .reduce((a, b) -> a + ", " + b).orElse("*");
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
