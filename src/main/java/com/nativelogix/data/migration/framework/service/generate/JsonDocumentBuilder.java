package com.nativelogix.data.migration.framework.service.generate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nativelogix.data.migration.framework.model.project.JsonColumnMapping;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import com.nativelogix.data.migration.framework.model.project.NamingCase;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Builds a formatted JSON string from a root row and its related child rows,
 * driven by the project's {@link JsonTableMapping} definitions.
 */
@Component
public class JsonDocumentBuilder {

    private static final DateTimeFormatter ISO_DATE     = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter ISO_DATETIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter prettyWriter = objectMapper.writerWithDefaultPrettyPrinter();
    private final JavaScriptFunctionExecutor jsExecutor;

    public JsonDocumentBuilder(JavaScriptFunctionExecutor jsExecutor) {
        this.jsExecutor = jsExecutor;
    }

    /**
     * A single queried row together with any inline children that must be
     * nested inside it in the JSON output.
     */
    public record MappedRow(
            Map<String, Object> row,
            Map<JsonTableMapping, List<MappedRow>> inlineChildren) {}

    /**
     * Builds a single JSON document string for one root row.
     *
     * @param rootMapping  the root table mapping (mappingType = "RootObject")
     * @param rootRow      column name → value map for the root row
     * @param childData    root-level child mappings → their MappedRows (with inline children attached)
     * @param casing       naming convention to apply to property names (null = use as-is)
     * @return pretty-printed JSON string
     */
    public String build(JsonTableMapping rootMapping,
                        Map<String, Object> rootRow,
                        Map<JsonTableMapping, List<MappedRow>> childData,
                        NamingCase casing) throws Exception {

        ObjectNode root = objectMapper.createObjectNode();
        applyColumns(root, rootMapping.getColumns(), rootRow, casing);

        if (childData != null) {
            for (Map.Entry<JsonTableMapping, List<MappedRow>> entry : childData.entrySet()) {
                JsonTableMapping childMapping = entry.getKey();
                List<MappedRow> mappedRows   = entry.getValue();

                // Embed: skip the wrapper key, apply properties directly to root
                if (childMapping.isEmbed()) {
                    if (!mappedRows.isEmpty()) {
                        MappedRow mr = mappedRows.get(0);
                        applyColumns(root, childMapping.getColumns(), mr.row(), casing);
                        buildInlineChildren(root, mr.inlineChildren(), casing);
                    }
                    continue;
                }

                String key = childMapping.getJsonName();

                if ("Array".equals(childMapping.getMappingType())) {
                    ArrayNode arrayNode = root.putArray(key);
                    for (MappedRow mr : mappedRows) {
                        ObjectNode childObj = objectMapper.createObjectNode();
                        applyColumns(childObj, childMapping.getColumns(), mr.row(), casing);
                        buildInlineChildren(childObj, mr.inlineChildren(), casing);
                        arrayNode.add(childObj);
                    }
                } else {
                    // InlineObject: single nested object (first row only); null when no matching row
                    if (!mappedRows.isEmpty()) {
                        MappedRow mr = mappedRows.get(0);
                        ObjectNode childObj = root.putObject(key);
                        applyColumns(childObj, childMapping.getColumns(), mr.row(), casing);
                        buildInlineChildren(childObj, mr.inlineChildren(), casing);
                    } else {
                        root.putNull(key);
                    }
                }
            }
        }

        return prettyWriter.writeValueAsString(root);
    }

    // -------------------------------------------------------------------------

    private void buildInlineChildren(ObjectNode parentNode,
                                     Map<JsonTableMapping, List<MappedRow>> inlineData,
                                     NamingCase casing) throws Exception {
        if (inlineData == null || inlineData.isEmpty()) return;

        for (Map.Entry<JsonTableMapping, List<MappedRow>> entry : inlineData.entrySet()) {
            JsonTableMapping mapping = entry.getKey();
            List<MappedRow>  rows    = entry.getValue();

            if (mapping.isEmbed()) {
                // Embed: skip the wrapper key, apply properties directly to parentNode
                if (!rows.isEmpty()) {
                    MappedRow mr = rows.get(0);
                    applyColumns(parentNode, mapping.getColumns(), mr.row(), casing);
                    buildInlineChildren(parentNode, mr.inlineChildren(), casing);
                }
                continue;
            }

            String key = mapping.getJsonName();

            if ("Array".equals(mapping.getMappingType())) {
                ArrayNode arrayNode = parentNode.putArray(key);
                for (MappedRow mr : rows) {
                    ObjectNode obj = objectMapper.createObjectNode();
                    applyColumns(obj, mapping.getColumns(), mr.row(), casing);
                    buildInlineChildren(obj, mr.inlineChildren(), casing);
                    arrayNode.add(obj);
                }
            } else {
                if (!rows.isEmpty()) {
                    MappedRow mr = rows.get(0);
                    ObjectNode obj = parentNode.putObject(key);
                    applyColumns(obj, mapping.getColumns(), mr.row(), casing);
                    buildInlineChildren(obj, mr.inlineChildren(), casing);
                } else {
                    parentNode.putNull(key);
                }
            }
        }
    }

    private void applyColumns(ObjectNode node,
                               List<JsonColumnMapping> columns,
                               Map<String, Object> row,
                               NamingCase casing) {
        if (columns == null) return;

        for (JsonColumnMapping col : columns) {
            String key = col.getJsonKey();

            boolean isCustom = "CUSTOM".equals(col.getMappingType())
                    || col.getSourceColumn() == null || col.getSourceColumn().isBlank();
            if (isCustom) {
                if (col.getCustomFunction() != null && !col.getCustomFunction().isBlank()) {
                    String value = jsExecutor.evaluate(col.getCustomFunction(), row);
                    if (value != null) {
                        putTyped(node, key, value, col.getJsonType());
                    }
                }
                continue;
            }

            Object rawValue = row.get(col.getSourceColumn());
            if (rawValue == null) continue;

            putTyped(node, key, rawValue, col.getJsonType());
        }
    }

    private void putTyped(ObjectNode node, String key, Object value, String jsonType) {
        if ("boolean".equals(jsonType)) {
            if (value instanceof Boolean b) { node.put(key, b); return; }
            String s = value.toString().trim();
            node.put(key, "1".equals(s) || "true".equalsIgnoreCase(s));
            return;
        }

        if ("number".equals(jsonType)) {
            if (value instanceof BigDecimal bd) { node.put(key, bd); return; }
            if (value instanceof Double d)      { node.put(key, d); return; }
            if (value instanceof Float f)       { node.put(key, f); return; }
            if (value instanceof Long l)        { node.put(key, l); return; }
            if (value instanceof Integer i)     { node.put(key, i); return; }
            if (value instanceof Number n)      { node.put(key, new BigDecimal(n.toString())); return; }
            try { node.put(key, new BigDecimal(value.toString())); return; } catch (NumberFormatException ignored) {}
        }

        // string (default) — with date/datetime formatting
        if (value instanceof Date d)           { node.put(key, d.toLocalDate().format(ISO_DATE)); return; }
        if (value instanceof Timestamp ts)     { node.put(key, ts.toLocalDateTime().format(ISO_DATETIME)); return; }
        if (value instanceof LocalDate ld)     { node.put(key, ld.format(ISO_DATE)); return; }
        if (value instanceof LocalDateTime ldt){ node.put(key, ldt.format(ISO_DATETIME)); return; }

        node.put(key, value.toString());
    }
}
