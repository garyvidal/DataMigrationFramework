package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.DocumentModel;
import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlNamespace;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class XsltBuilderTest {

    private final XsltBuilder builder = new XsltBuilder();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private XmlTableMapping rootMapping(String xmlName, String sourceTable,
                                        List<XmlColumnMapping> columns) {
        XmlTableMapping m = new XmlTableMapping();
        m.setId("root-id");
        m.setXmlName(xmlName);
        m.setSourceTable(sourceTable);
        m.setMappingType("RootElement");
        m.setColumns(columns);
        return m;
    }

    private XmlTableMapping childMapping(String id, String xmlName, String sourceTable,
                                         String mappingType, List<XmlColumnMapping> columns) {
        XmlTableMapping m = new XmlTableMapping();
        m.setId(id);
        m.setXmlName(xmlName);
        m.setSourceTable(sourceTable);
        m.setMappingType(mappingType);
        m.setColumns(columns);
        return m;
    }

    private XmlColumnMapping col(String xmlName) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName(xmlName);
        c.setMappingType("Element");
        return c;
    }

    private XmlColumnMapping attrCol(String xmlName) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName(xmlName);
        c.setMappingType("ElementAttribute");
        return c;
    }

    private XmlColumnMapping customCol(String xmlName) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName(xmlName);
        c.setMappingType("CUSTOM");
        return c;
    }

    private DocumentModel model(XmlTableMapping root, List<XmlTableMapping> elements) {
        DocumentModel m = new DocumentModel();
        m.setRoot(root);
        m.setElements(elements);
        return m;
    }

    // ── Stylesheet root ───────────────────────────────────────────────────────

    @Test
    void build_emitsXsltStylesheetElement() throws Exception {
        String xslt = builder.build(model(rootMapping("Employee", "employee", List.of()), null), null);
        assertTrue(xslt.contains("xsl:stylesheet"));
        assertTrue(xslt.contains("version=\"2.0\""));
        assertTrue(xslt.contains("xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\""));
    }

    @Test
    void build_emitsXslOutputElement() throws Exception {
        String xslt = builder.build(model(rootMapping("Employee", "employee", List.of()), null), null);
        assertTrue(xslt.contains("xsl:output"));
        assertTrue(xslt.contains("method=\"xml\""));
    }

    // ── Entry-point template ──────────────────────────────────────────────────

    @Test
    void build_emitsEntryTemplateMatchingRoot() throws Exception {
        String xslt = builder.build(model(rootMapping("Employee", "employee", List.of()), null), null);
        assertTrue(xslt.contains("match=\"/\""));
    }

    @Test
    void build_entryTemplateCreatesRootElement() throws Exception {
        String xslt = builder.build(model(rootMapping("Employee", "employee", List.of()), null), null);
        assertTrue(xslt.contains("Employee"));
    }

    @Test
    void build_entryTemplateCallsRootNamedTemplate() throws Exception {
        String xslt = builder.build(model(rootMapping("Employee", "employee", List.of()), null), null);
        // Root template name follows "tpl-{table}-{xmlName}" convention
        assertTrue(xslt.contains("name=\"tpl-employee-employee\""));
    }

    // ── Named template per mapping ────────────────────────────────────────────

    @Test
    void build_rootMappingGetsNamedTemplate() throws Exception {
        String xslt = builder.build(model(rootMapping("Order", "order", List.of()), null), null);
        assertTrue(xslt.contains("name=\"tpl-order-order\""));
    }

    @Test
    void build_childMappingGetsOwnNamedTemplate() throws Exception {
        XmlTableMapping root  = rootMapping("Order", "order", List.of());
        XmlTableMapping child = childMapping("c1", "Item", "order_item", "Elements", List.of());

        String xslt = builder.build(model(root, List.of(child)), null);
        assertTrue(xslt.contains("name=\"tpl-order_item-item\""));
    }

    @Test
    void build_inlineMappingGetsOwnNamedTemplate() throws Exception {
        XmlTableMapping root   = rootMapping("Order", "order", List.of());
        XmlTableMapping inline = childMapping("inline-id", "Address", "address", "InlineElement", List.of());
        inline.setParentRef("root-id");

        String xslt = builder.build(model(root, List.of(inline)), null);
        assertTrue(xslt.contains("name=\"tpl-address-address\""));
    }

    // ── Template name sanitization ────────────────────────────────────────────

    @Test
    void build_tableNameWithSpecialChars_isSanitizedInTemplateName() throws Exception {
        // Spaces and dots in table names must become underscores (valid NCName)
        XmlTableMapping root = rootMapping("My.Element", "my.table name", List.of());
        String xslt = builder.build(model(root, null), null);
        // Dots and spaces → underscores, lowercased
        assertTrue(xslt.contains("tpl-my_table_name-my_element"));
    }

    // ── Column copy-of expressions ────────────────────────────────────────────

    @Test
    void build_elementColumn_emitsCopyOf() throws Exception {
        XmlTableMapping root = rootMapping("Employee", "employee", List.of(col("FirstName")));
        String xslt = builder.build(model(root, null), null);
        assertTrue(xslt.contains("xsl:copy-of"));
        assertTrue(xslt.contains("select=\"FirstName\""));
    }

    @Test
    void build_attributeColumn_emitsXslAttribute() throws Exception {
        XmlTableMapping root = rootMapping("Employee", "employee", List.of(attrCol("version")));
        String xslt = builder.build(model(root, null), null);
        assertTrue(xslt.contains("xsl:attribute"));
        assertTrue(xslt.contains("name=\"version\""));
        assertTrue(xslt.contains("select=\"@version\""));
    }

    @Test
    void build_customColumn_emitsCommentPlaceholder() throws Exception {
        XmlTableMapping root = rootMapping("Record", "record", List.of(customCol("Computed")));
        String xslt = builder.build(model(root, null), null);
        assertTrue(xslt.contains("CUSTOM element: Computed"));
    }

    @Test
    void build_columnWithBlankName_isSkipped() throws Exception {
        XmlColumnMapping blank = new XmlColumnMapping();
        blank.setXmlName("  ");
        blank.setMappingType("Element");
        XmlTableMapping root = rootMapping("Record", "record", List.of(blank, col("Id")));
        String xslt = builder.build(model(root, null), null);
        // Id should appear as copy-of; blank should not produce a broken select=""
        assertTrue(xslt.contains("select=\"Id\""));
        assertFalse(xslt.contains("select=\"  \""));
    }

    // ── CUSTOM table mapping ──────────────────────────────────────────────────

    @Test
    void build_customTableMapping_emitsCommentOnlyTemplate() throws Exception {
        XmlTableMapping root   = rootMapping("Record", "record", List.of());
        XmlTableMapping custom = childMapping("cust-id", "Computed", "computed_table", "CUSTOM", null);

        String xslt = builder.build(model(root, List.of(custom)), null);
        assertTrue(xslt.contains("CUSTOM mapping 'Computed'"));
        assertTrue(xslt.contains("name=\"tpl-computed_table-computed\""));
    }

    // ── Wrapper element ───────────────────────────────────────────────────────

    @Test
    void build_childWithWrapInParent_emitsWrapperInTemplate() throws Exception {
        XmlTableMapping root  = rootMapping("Order", "order", List.of());
        XmlTableMapping child = childMapping("c1", "Item", "item", "Elements", List.of(col("ItemId")));
        child.setWrapInParent(true);
        child.setWrapperElementName("Items");

        String xslt = builder.build(model(root, List.of(child)), null);
        assertTrue(xslt.contains("Items"));
        assertTrue(xslt.contains("Item"));
    }

    // ── Embed ─────────────────────────────────────────────────────────────────

    @Test
    void build_embeddedMapping_doesNotWrapInElement() throws Exception {
        XmlTableMapping root     = rootMapping("Person", "person", List.of(col("Id")));
        XmlTableMapping embedded = childMapping("emb-id", "AddressBlock", "address",
                "InlineElement", List.of(col("City")));
        embedded.setEmbed(true);
        embedded.setParentRef("root-id");

        String xslt = builder.build(model(root, List.of(embedded)), null);
        // City should appear as copy-of
        assertTrue(xslt.contains("select=\"City\""));
        // No for-each wrapping AddressBlock as an element
        assertFalse(xslt.contains("select=\"$context/AddressBlock\""));
    }

    // ── Namespace declarations ────────────────────────────────────────────────

    @Test
    void build_namespaceWithPrefix_emitsXmlnsOnStylesheet() throws Exception {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("dc");
        ns.setUri("http://purl.org/dc/elements/1.1/");

        String xslt = builder.build(
                model(rootMapping("Record", "record", List.of()), null), List.of(ns));
        assertTrue(xslt.contains("xmlns:dc=\"http://purl.org/dc/elements/1.1/\""));
    }

    @Test
    void build_namespaceEmitsXslNamespaceInEntryTemplate() throws Exception {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("dc");
        ns.setUri("http://purl.org/dc/elements/1.1/");

        String xslt = builder.build(
                model(rootMapping("Record", "record", List.of()), null), List.of(ns));
        assertTrue(xslt.contains("xsl:namespace"));
        assertTrue(xslt.contains("'http://purl.org/dc/elements/1.1/'"));
    }

    // ── Context parameter ─────────────────────────────────────────────────────

    @Test
    void build_eachNamedTemplateHasContextParam() throws Exception {
        XmlTableMapping root  = rootMapping("Order", "order", List.of());
        XmlTableMapping child = childMapping("c1", "Item", "item", "Elements", List.of());

        String xslt = builder.build(model(root, List.of(child)), null);
        // Every named template should declare a context param
        long paramCount = xslt.lines()
                .filter(l -> l.contains("name=\"context\""))
                .count();
        // At least one per non-custom template (root + child = 2)
        assertTrue(paramCount >= 2);
    }

    // ── Null / empty inputs ───────────────────────────────────────────────────

    @Test
    void build_noElements_doesNotThrow() {
        assertDoesNotThrow(() ->
                builder.build(model(rootMapping("Root", "root", List.of()), null), null));
    }

    @Test
    void build_nullColumns_doesNotThrow() {
        assertDoesNotThrow(() ->
                builder.build(model(rootMapping("Root", "root", null), null), null));
    }

    @Test
    void build_nullNamespaces_doesNotThrow() {
        assertDoesNotThrow(() ->
                builder.build(model(rootMapping("Root", "root", List.of()), null), null));
    }
}
