package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlNamespace;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
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

class XmlDocumentBuilderTest {

    private final JavaScriptFunctionExecutor jsExecutor = new JavaScriptFunctionExecutor();
    private final XmlDocumentBuilder builder = new XmlDocumentBuilder(jsExecutor);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private XmlTableMapping rootMapping(String xmlName, List<XmlColumnMapping> columns) {
        XmlTableMapping m = new XmlTableMapping();
        m.setXmlName(xmlName);
        m.setMappingType("RootElement");
        m.setColumns(columns);
        return m;
    }

    private XmlColumnMapping col(String sourceCol, String xmlName) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setSourceColumn(sourceCol);
        c.setXmlName(xmlName);
        c.setMappingType("Element");
        return c;
    }

    private XmlColumnMapping typedCol(String sourceCol, String xmlName, String xmlType) {
        XmlColumnMapping c = col(sourceCol, xmlName);
        c.setXmlType(xmlType);
        return c;
    }

    private XmlColumnMapping customCol(String xmlName, String function) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName(xmlName);
        c.setMappingType("CUSTOM");
        c.setCustomFunction(function);
        return c;
    }

    private XmlColumnMapping prefixedCol(String sourceCol, String xmlName, String prefix) {
        XmlColumnMapping c = col(sourceCol, xmlName);
        c.setNamespacePrefix(prefix);
        return c;
    }

    // ── XML declaration and root element ─────────────────────────────────────

    @Test
    void build_alwaysEmitsXmlDeclaration() {
        String xml = builder.build(rootMapping("Employee", List.of()), Map.of(), null, null, null);
        assertTrue(xml.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    }

    @Test
    void build_emitsRootElementOpenAndClose() {
        String xml = builder.build(rootMapping("Employee", List.of()), Map.of(), null, null, null);
        assertTrue(xml.contains("<Employee>"));
        assertTrue(xml.contains("</Employee>"));
    }

    // ── Column mapping ────────────────────────────────────────────────────────

    @Test
    void build_simpleColumn_emitsElement() {
        XmlTableMapping mapping = rootMapping("Employee", List.of(col("first_name", "FirstName")));
        String xml = builder.build(mapping, Map.of("first_name", "Gary"), null, null, null);
        assertTrue(xml.contains("<FirstName>Gary</FirstName>"));
    }

    @Test
    void build_nullColumnValue_skipsElement() {
        Map<String, Object> row = new java.util.HashMap<>();
        row.put("middle_name", null);
        XmlTableMapping mapping = rootMapping("Employee", List.of(col("middle_name", "MiddleName")));
        String xml = builder.build(mapping, row, null, null, null);
        assertFalse(xml.contains("MiddleName"));
    }

    @Test
    void build_missingColumnKey_skipsElement() {
        XmlTableMapping mapping = rootMapping("Employee", List.of(col("missing_col", "Missing")));
        String xml = builder.build(mapping, Map.of(), null, null, null);
        assertFalse(xml.contains("Missing"));
    }

    @Test
    void build_nullColumns_doesNotThrow() {
        XmlTableMapping mapping = rootMapping("Employee", null);
        assertDoesNotThrow(() -> builder.build(mapping, Map.of(), null, null, null));
    }

    // ── XML special character escaping ────────────────────────────────────────

    @Test
    void build_ampersandInValue_isEscaped() {
        XmlTableMapping mapping = rootMapping("Co", List.of(col("name", "Name")));
        String xml = builder.build(mapping, Map.of("name", "Foo & Bar"), null, null, null);
        assertTrue(xml.contains("Foo &amp; Bar"));
        assertFalse(xml.contains("Foo & Bar"));
    }

    @Test
    void build_lessThanInValue_isEscaped() {
        XmlTableMapping mapping = rootMapping("Co", List.of(col("expr", "Expr")));
        String xml = builder.build(mapping, Map.of("expr", "a < b"), null, null, null);
        assertTrue(xml.contains("a &lt; b"));
    }

    @Test
    void build_greaterThanInValue_isEscaped() {
        XmlTableMapping mapping = rootMapping("Co", List.of(col("expr", "Expr")));
        String xml = builder.build(mapping, Map.of("expr", "a > b"), null, null, null);
        assertTrue(xml.contains("a &gt; b"));
    }

    // ── Type formatting ───────────────────────────────────────────────────────

    @Test
    void build_xsDateFromSqlDate_formatsAsIsoDate() {
        XmlTableMapping mapping = rootMapping("R", List.of(typedCol("dob", "DateOfBirth", "xs:date")));
        String xml = builder.build(mapping, Map.of("dob", Date.valueOf("2000-06-15")), null, null, null);
        assertTrue(xml.contains("<DateOfBirth>2000-06-15</DateOfBirth>"));
    }

    @Test
    void build_xsDateFromLocalDate_formatsAsIsoDate() {
        XmlTableMapping mapping = rootMapping("R", List.of(typedCol("dob", "DateOfBirth", "xs:date")));
        String xml = builder.build(mapping, Map.of("dob", LocalDate.of(1990, 1, 1)), null, null, null);
        assertTrue(xml.contains("<DateOfBirth>1990-01-01</DateOfBirth>"));
    }

    @Test
    void build_xsDateTimeFromTimestamp_formatsAsIsoDateTime() {
        XmlTableMapping mapping = rootMapping("R", List.of(typedCol("created", "Created", "xs:dateTime")));
        LocalDateTime ldt = LocalDateTime.of(2024, 3, 15, 10, 30, 0);
        String xml = builder.build(mapping, Map.of("created", Timestamp.valueOf(ldt)), null, null, null);
        assertTrue(xml.contains("<Created>2024-03-15T10:30:00</Created>"));
    }

    @Test
    void build_xsDecimalFromBigDecimal_stripsTrailingZeros() {
        XmlTableMapping mapping = rootMapping("R", List.of(typedCol("price", "Price", "xs:decimal")));
        String xml = builder.build(mapping, Map.of("price", new BigDecimal("19.500")), null, null, null);
        assertTrue(xml.contains("<Price>19.5</Price>"));
    }

    @Test
    void build_xsBooleanFromTrue_emitsTrue() {
        XmlTableMapping mapping = rootMapping("R", List.of(typedCol("active", "Active", "xs:boolean")));
        String xml = builder.build(mapping, Map.of("active", true), null, null, null);
        assertTrue(xml.contains("<Active>true</Active>"));
    }

    @Test
    void build_xsBooleanFromOne_emitsTrue() {
        XmlTableMapping mapping = rootMapping("R", List.of(typedCol("active", "Active", "xs:boolean")));
        String xml = builder.build(mapping, Map.of("active", "1"), null, null, null);
        assertTrue(xml.contains("<Active>true</Active>"));
    }

    // ── Custom (JS) column functions ──────────────────────────────────────────

    @Test
    void build_customColumnFunction_emitsComputedElement() {
        XmlColumnMapping custom = customCol("FullName", "row.first_name + ' ' + row.last_name");
        XmlTableMapping mapping = rootMapping("Employee", List.of(custom));
        String xml = builder.build(mapping, Map.of("first_name", "Jane", "last_name", "Doe"), null, null, null);
        assertTrue(xml.contains("<FullName>Jane Doe</FullName>"));
    }

    @Test
    void build_customColumnFunctionReturnsNull_skipsElement() {
        XmlColumnMapping custom = customCol("Optional", "null");
        XmlTableMapping mapping = rootMapping("R", List.of(custom));
        String xml = builder.build(mapping, Map.of(), null, null, null);
        assertFalse(xml.contains("Optional"));
    }

    // ── Namespace declarations ────────────────────────────────────────────────

    @Test
    void build_namespaceWithPrefix_emitsXmlnsOnRootElement() {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("dc");
        ns.setUri("http://purl.org/dc/elements/1.1/");

        XmlTableMapping mapping = rootMapping("Record", List.of());
        String xml = builder.build(mapping, Map.of(), null, null, List.of(ns));
        assertTrue(xml.contains("xmlns:dc=\"http://purl.org/dc/elements/1.1/\""));
    }

    @Test
    void build_defaultNamespace_emitsXmlnsWithoutPrefix() {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("");
        ns.setUri("urn:example:schema");

        XmlTableMapping mapping = rootMapping("Record", List.of());
        String xml = builder.build(mapping, Map.of(), null, null, List.of(ns));
        assertTrue(xml.contains("xmlns=\"urn:example:schema\""));
    }

    @Test
    void build_columnWithNamespacePrefix_emitsQualifiedTagName() {
        XmlColumnMapping c = prefixedCol("title", "title", "dc");
        XmlTableMapping mapping = rootMapping("Record", List.of(c));
        String xml = builder.build(mapping, Map.of("title", "Migration Guide"), null, null, null);
        assertTrue(xml.contains("<dc:title>Migration Guide</dc:title>"));
    }

    // ── Child mappings — wrapper element ──────────────────────────────────────

    @Test
    void build_childWithWrapper_emitsWrapperAroundChildren() {
        XmlTableMapping root = rootMapping("Order", List.of(col("order_id", "OrderId")));

        XmlTableMapping child = new XmlTableMapping();
        child.setXmlName("Item");
        child.setMappingType("Elements");
        child.setWrapInParent(true);
        child.setWrapperElementName("Items");
        child.setColumns(List.of(col("item_id", "ItemId")));

        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of(
                new XmlDocumentBuilder.MappedRow(Map.of("item_id", "101"), null),
                new XmlDocumentBuilder.MappedRow(Map.of("item_id", "102"), null)
        ));

        String xml = builder.build(root, Map.of("order_id", "ORD-1"), childData, null, null);

        assertTrue(xml.contains("<Items>"));
        assertTrue(xml.contains("</Items>"));
        assertTrue(xml.contains("<Item>"));
        assertTrue(xml.contains("<ItemId>101</ItemId>"));
        assertTrue(xml.contains("<ItemId>102</ItemId>"));
    }

    @Test
    void build_childWithoutWrapper_emitsChildrenDirectlyUnderRoot() {
        XmlTableMapping root = rootMapping("Record", List.of());

        XmlTableMapping child = new XmlTableMapping();
        child.setXmlName("Phone");
        child.setMappingType("Elements");
        child.setWrapInParent(false);
        child.setColumns(List.of(col("number", "Number")));

        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(child, List.of(
                new XmlDocumentBuilder.MappedRow(Map.of("number", "555-1234"), null)
        ));

        String xml = builder.build(root, Map.of(), childData, null, null);

        assertTrue(xml.contains("<Phone>"));
        assertTrue(xml.contains("<Number>555-1234</Number>"));
        assertFalse(xml.contains("<null>"));
    }

    // ── Embed ─────────────────────────────────────────────────────────────────

    @Test
    void build_embeddedChild_inlinesColumnsDirectlyIntoRoot() {
        XmlTableMapping root = rootMapping("Person", List.of(col("id", "Id")));

        XmlTableMapping embedded = new XmlTableMapping();
        embedded.setXmlName("AddressBlock");
        embedded.setMappingType("InlineElement");
        embedded.setEmbed(true);
        embedded.setColumns(List.of(col("city", "City")));

        Map<XmlTableMapping, List<XmlDocumentBuilder.MappedRow>> childData = new LinkedHashMap<>();
        childData.put(embedded, List.of(
                new XmlDocumentBuilder.MappedRow(Map.of("city", "Portland"), null)
        ));

        String xml = builder.build(root, Map.of("id", "1"), childData, null, null);

        assertTrue(xml.contains("<City>Portland</City>"));
        assertFalse(xml.contains("<AddressBlock>"));
    }

}
