package com.nativelogix.data.migration.framework.service.generate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nativelogix.data.migration.framework.model.project.JsonColumnMapping;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonDocumentBuilderTest {

    private final JavaScriptFunctionExecutor jsExecutor = new JavaScriptFunctionExecutor();
    private final JsonDocumentBuilder builder = new JsonDocumentBuilder(jsExecutor);
    private final ObjectMapper mapper = new ObjectMapper();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JsonTableMapping rootMapping(String jsonName, List<JsonColumnMapping> columns) {
        JsonTableMapping m = new JsonTableMapping();
        m.setJsonName(jsonName);
        m.setMappingType("RootObject");
        m.setColumns(columns);
        return m;
    }

    private JsonColumnMapping col(String sourceCol, String jsonKey) {
        JsonColumnMapping c = new JsonColumnMapping();
        c.setSourceColumn(sourceCol);
        c.setJsonKey(jsonKey);
        c.setMappingType("Property");
        return c;
    }

    private JsonColumnMapping typedCol(String sourceCol, String jsonKey, String jsonType) {
        JsonColumnMapping c = col(sourceCol, jsonKey);
        c.setJsonType(jsonType);
        return c;
    }

    private JsonColumnMapping customCol(String jsonKey, String function) {
        JsonColumnMapping c = new JsonColumnMapping();
        c.setJsonKey(jsonKey);
        c.setMappingType("CUSTOM");
        c.setCustomFunction(function);
        return c;
    }

    private JsonNode parse(String json) throws Exception {
        return mapper.readTree(json);
    }

    // ── Basic structure ───────────────────────────────────────────────────────

    @Test
    void build_emptyMapping_returnsDocumentWithRootKey() throws Exception {
        String json = builder.build(rootMapping("root", List.of()), Map.of(), null, null);
        JsonNode doc = parse(json);
        assertTrue(doc.isObject());
        assertTrue(doc.has("root"));
        assertEquals(0, doc.get("root").size());
    }

    @Test
    void build_simpleStringColumn_emitsProperty() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(col("first_name", "firstName")));
        String json = builder.build(mapping, Map.of("first_name", "Gary"), null, null);
        assertEquals("Gary", parse(json).get("root").get("firstName").asText());
    }

    @Test
    void build_nullColumnValue_skipsProperty() throws Exception {
        Map<String, Object> row = new java.util.HashMap<>();
        row.put("middle_name", null);
        JsonTableMapping mapping = rootMapping("root", List.of(col("middle_name", "middleName")));
        String json = builder.build(mapping, row, null, null);
        assertFalse(parse(json).get("root").has("middleName"));
    }

    @Test
    void build_missingColumnKey_skipsProperty() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(col("missing", "missing")));
        String json = builder.build(mapping, Map.of(), null, null);
        assertFalse(parse(json).get("root").has("missing"));
    }

    @Test
    void build_nullColumns_doesNotThrow() {
        JsonTableMapping mapping = rootMapping("root", null);
        assertDoesNotThrow(() -> builder.build(mapping, Map.of(), null, null));
    }

    // ── Type mapping ──────────────────────────────────────────────────────────

    @Test
    void build_numberTypeFromInteger_emitsNumericNode() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(typedCol("qty", "quantity", "number")));
        String json = builder.build(mapping, Map.of("qty", 42), null, null);
        JsonNode node = parse(json).get("root").get("quantity");
        assertTrue(node.isNumber());
        assertEquals(42, node.asInt());
    }

    @Test
    void build_numberTypeFromBigDecimal_emitsNumericNode() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(typedCol("price", "price", "number")));
        String json = builder.build(mapping, Map.of("price", new BigDecimal("19.99")), null, null);
        JsonNode node = parse(json).get("root").get("price");
        assertTrue(node.isNumber());
        assertEquals(new BigDecimal("19.99"), node.decimalValue());
    }

    @Test
    void build_booleanTypeFromBoolean_emitsBooleanNode() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(typedCol("active", "active", "boolean")));
        String json = builder.build(mapping, Map.of("active", true), null, null);
        JsonNode node = parse(json).get("root").get("active");
        assertTrue(node.isBoolean());
        assertTrue(node.asBoolean());
    }

    @Test
    void build_booleanTypeFromOne_emitsTrue() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(typedCol("flag", "flag", "boolean")));
        String json = builder.build(mapping, Map.of("flag", "1"), null, null);
        assertTrue(parse(json).get("root").get("flag").asBoolean());
    }

    @Test
    void build_dateFromSqlDate_formatsAsIsoDate() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(col("dob", "dateOfBirth")));
        String json = builder.build(mapping, Map.of("dob", Date.valueOf("2000-06-15")), null, null);
        assertEquals("2000-06-15", parse(json).get("root").get("dateOfBirth").asText());
    }

    @Test
    void build_dateFromLocalDate_formatsAsIsoDate() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(col("dob", "dateOfBirth")));
        String json = builder.build(mapping, Map.of("dob", LocalDate.of(1990, 1, 1)), null, null);
        assertEquals("1990-01-01", parse(json).get("root").get("dateOfBirth").asText());
    }

    @Test
    void build_dateTimeFromTimestamp_formatsAsIsoDateTime() throws Exception {
        JsonTableMapping mapping = rootMapping("root", List.of(col("created", "createdAt")));
        LocalDateTime ldt = LocalDateTime.of(2024, 3, 15, 10, 30, 0);
        String json = builder.build(mapping, Map.of("created", Timestamp.valueOf(ldt)), null, null);
        assertEquals("2024-03-15T10:30:00", parse(json).get("root").get("createdAt").asText());
    }

    // ── Custom (JS) column functions ──────────────────────────────────────────

    @Test
    void build_customColumnFunction_emitsComputedProperty() throws Exception {
        JsonColumnMapping custom = customCol("fullName", "row.first_name + ' ' + row.last_name");
        JsonTableMapping mapping = rootMapping("root", List.of(custom));
        String json = builder.build(mapping, Map.of("first_name", "Jane", "last_name", "Doe"), null, null);
        assertEquals("Jane Doe", parse(json).get("root").get("fullName").asText());
    }

    @Test
    void build_customColumnFunctionReturnsNull_skipsProperty() throws Exception {
        JsonColumnMapping custom = customCol("optional", "null");
        JsonTableMapping mapping = rootMapping("root", List.of(custom));
        String json = builder.build(mapping, Map.of(), null, null);
        assertFalse(parse(json).get("root").has("optional"));
    }

    // ── Array child mapping ───────────────────────────────────────────────────

    @Test
    void build_arrayChild_emitsJsonArray() throws Exception {
        JsonTableMapping root = rootMapping("root", List.of(col("order_id", "orderId")));

        JsonTableMapping child = new JsonTableMapping();
        child.setJsonName("items");
        child.setMappingType("Array");
        child.setColumns(List.of(col("item_id", "itemId"), col("qty", "qty")));

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of(
                new JsonDocumentBuilder.MappedRow(Map.of("item_id", "A1", "qty", "2"), null),
                new JsonDocumentBuilder.MappedRow(Map.of("item_id", "B2", "qty", "5"), null)
        ));

        JsonNode node = parse(builder.build(root, Map.of("order_id", "ORD-1"), childData, null)).get("root");

        assertTrue(node.get("items").isArray());
        assertEquals(2, node.get("items").size());
        assertEquals("A1", node.get("items").get(0).get("itemId").asText());
        assertEquals("B2", node.get("items").get(1).get("itemId").asText());
    }

    @Test
    void build_emptyArrayChild_emitsEmptyArray() throws Exception {
        JsonTableMapping root = rootMapping("root", List.of());

        JsonTableMapping child = new JsonTableMapping();
        child.setJsonName("tags");
        child.setMappingType("Array");
        child.setColumns(List.of());

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of());

        JsonNode node = parse(builder.build(root, Map.of(), childData, null)).get("root");
        assertTrue(node.get("tags").isArray());
        assertEquals(0, node.get("tags").size());
    }

    // ── InlineObject child mapping ────────────────────────────────────────────

    @Test
    void build_inlineObjectChild_emitsNestedObject() throws Exception {
        JsonTableMapping root = rootMapping("root", List.of(col("id", "id")));

        JsonTableMapping child = new JsonTableMapping();
        child.setJsonName("address");
        child.setMappingType("InlineObject");
        child.setColumns(List.of(col("city", "city"), col("zip", "zip")));

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of(
                new JsonDocumentBuilder.MappedRow(Map.of("city", "Portland", "zip", "97201"), null)
        ));

        JsonNode node = parse(builder.build(root, Map.of("id", "1"), childData, null)).get("root");
        JsonNode address = node.get("address");
        assertNotNull(address);
        assertTrue(address.isObject());
        assertEquals("Portland", address.get("city").asText());
        assertEquals("97201", address.get("zip").asText());
    }

    @Test
    void build_inlineObjectChildNoRows_emitsNullProperty() throws Exception {
        JsonTableMapping root = rootMapping("root", List.of());

        JsonTableMapping child = new JsonTableMapping();
        child.setJsonName("profile");
        child.setMappingType("InlineObject");
        child.setColumns(List.of());

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of());  // no matching rows

        JsonNode node = parse(builder.build(root, Map.of(), childData, null)).get("root");
        assertTrue(node.get("profile").isNull());
    }

    // ── Embed ─────────────────────────────────────────────────────────────────

    @Test
    void build_embeddedChild_inlinesPropertiesDirectlyIntoRoot() throws Exception {
        JsonTableMapping root = rootMapping("root", List.of(col("id", "id")));

        JsonTableMapping embedded = new JsonTableMapping();
        embedded.setJsonName("addressBlock");
        embedded.setMappingType("InlineObject");
        embedded.setEmbed(true);
        embedded.setColumns(List.of(col("city", "city")));

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(embedded, List.of(
                new JsonDocumentBuilder.MappedRow(Map.of("city", "Seattle"), null)
        ));

        JsonNode node = parse(builder.build(root, Map.of("id", "1"), childData, null)).get("root");

        // city should be a top-level property under root, not nested under "addressBlock"
        assertEquals("Seattle", node.get("city").asText());
        assertFalse(node.has("addressBlock"));
    }

    // ── Nested inline children ────────────────────────────────────────────────

    @Test
    void build_nestedInlineChildren_areRenderedCorrectly() throws Exception {
        JsonTableMapping root = rootMapping("root", List.of(col("id", "id")));

        JsonTableMapping child = new JsonTableMapping();
        child.setJsonName("department");
        child.setMappingType("InlineObject");
        child.setColumns(List.of(col("dept_name", "name")));

        JsonTableMapping grandchild = new JsonTableMapping();
        grandchild.setJsonName("manager");
        grandchild.setMappingType("InlineObject");
        grandchild.setColumns(List.of(col("mgr_name", "name")));

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> grandchildData = new LinkedHashMap<>();
        grandchildData.put(grandchild, List.of(
                new JsonDocumentBuilder.MappedRow(Map.of("mgr_name", "Alice"), null)
        ));

        Map<JsonTableMapping, List<JsonDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of(
                new JsonDocumentBuilder.MappedRow(Map.of("dept_name", "Engineering"), grandchildData)
        ));

        JsonNode node = parse(builder.build(root, Map.of("id", "42"), childData, null)).get("root");

        assertEquals("Engineering", node.get("department").get("name").asText());
        assertEquals("Alice", node.get("department").get("manager").get("name").asText());
    }
}
