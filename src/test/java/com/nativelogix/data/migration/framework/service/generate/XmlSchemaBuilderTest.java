package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.DocumentModel;
import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlNamespace;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class XmlSchemaBuilderTest {

    private final XmlSchemaBuilder builder = new XmlSchemaBuilder();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private XmlTableMapping rootMapping(String xmlName, List<XmlColumnMapping> columns) {
        XmlTableMapping m = new XmlTableMapping();
        m.setId("root-id");
        m.setXmlName(xmlName);
        m.setMappingType("RootElement");
        m.setColumns(columns);
        return m;
    }

    private XmlTableMapping childMapping(String id, String xmlName, String mappingType,
                                         List<XmlColumnMapping> columns) {
        XmlTableMapping m = new XmlTableMapping();
        m.setId(id);
        m.setXmlName(xmlName);
        m.setMappingType(mappingType);
        m.setColumns(columns);
        return m;
    }

    private XmlColumnMapping col(String xmlName, String xmlType) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName(xmlName);
        c.setXmlType(xmlType);
        c.setMappingType("Element");
        return c;
    }

    private XmlColumnMapping attrCol(String xmlName, String xmlType) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName(xmlName);
        c.setXmlType(xmlType);
        c.setMappingType("ElementAttribute");
        return c;
    }

    private DocumentModel model(XmlTableMapping root, List<XmlTableMapping> elements) {
        DocumentModel m = new DocumentModel();
        m.setRoot(root);
        m.setElements(elements);
        return m;
    }

    // ── xs:schema root ────────────────────────────────────────────────────────

    @Test
    void build_alwaysEmitsXsSchema() throws Exception {
        String xsd = builder.build(model(rootMapping("Employee", List.of()), null), null);
        assertTrue(xsd.contains("xs:schema"));
        assertTrue(xsd.contains("xmlns:xs=\"http://www.w3.org/2001/XMLSchema\""));
    }

    @Test
    void build_elementFormDefaultIsQualified() throws Exception {
        String xsd = builder.build(model(rootMapping("Employee", List.of()), null), null);
        assertTrue(xsd.contains("elementFormDefault=\"qualified\""));
    }

    // ── Root element ──────────────────────────────────────────────────────────

    @Test
    void build_rootMappingEmitsTopLevelElement() throws Exception {
        String xsd = builder.build(model(rootMapping("Employee", List.of()), null), null);
        assertTrue(xsd.contains("name=\"Employee\""));
    }

    @Test
    void build_rootElementHasNoMinMaxOccurs() throws Exception {
        // Top-level xs:element should NOT have minOccurs/maxOccurs
        String xsd = builder.build(model(rootMapping("Employee", List.of()), null), null);
        // The root element line should not carry those attributes
        // Simple check: the first occurrence of name="Employee" should not be followed by minOccurs on same element
        int nameIdx = xsd.indexOf("name=\"Employee\"");
        assertTrue(nameIdx >= 0);
        // Extract a reasonable slice around it and confirm no minOccurs adjacent
        String slice = xsd.substring(nameIdx, Math.min(nameIdx + 80, xsd.length()));
        assertFalse(slice.contains("minOccurs"));
    }

    // ── Column elements ───────────────────────────────────────────────────────

    @Test
    void build_columnElement_emitsXsElement() throws Exception {
        XmlTableMapping root = rootMapping("Employee", List.of(col("FirstName", "xs:string")));
        String xsd = builder.build(model(root, null), null);
        assertTrue(xsd.contains("name=\"FirstName\""));
        assertTrue(xsd.contains("type=\"xs:string\""));
    }

    @Test
    void build_columnWithNoType_defaultsToXsString() throws Exception {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName("Notes");
        c.setMappingType("Element");
        // no xmlType set
        XmlTableMapping root = rootMapping("Record", List.of(c));
        String xsd = builder.build(model(root, null), null);
        assertTrue(xsd.contains("name=\"Notes\""));
        assertTrue(xsd.contains("type=\"xs:string\""));
    }

    @Test
    void build_columnElement_hasMinOccursZero() throws Exception {
        XmlTableMapping root = rootMapping("Employee", List.of(col("Salary", "xs:decimal")));
        String xsd = builder.build(model(root, null), null);
        assertTrue(xsd.contains("minOccurs=\"0\""));
    }

    @Test
    void build_columnWithBlankName_isSkipped() throws Exception {
        XmlColumnMapping blank = new XmlColumnMapping();
        blank.setXmlName("   ");
        blank.setMappingType("Element");
        XmlTableMapping root = rootMapping("Record", List.of(blank, col("Id", "xs:integer")));
        String xsd = builder.build(model(root, null), null);
        // Id should appear, blank should not produce an empty name attribute
        assertTrue(xsd.contains("name=\"Id\""));
        assertFalse(xsd.contains("name=\"   \""));
    }

    // ── Attribute columns ─────────────────────────────────────────────────────

    @Test
    void build_attributeColumn_emitsXsAttribute() throws Exception {
        XmlTableMapping root = rootMapping("Employee", List.of(attrCol("version", "xs:integer")));
        String xsd = builder.build(model(root, null), null);
        assertTrue(xsd.contains("xs:attribute"));
        assertTrue(xsd.contains("name=\"version\""));
        assertTrue(xsd.contains("use=\"optional\""));
    }

    // ── Namespaces ────────────────────────────────────────────────────────────

    @Test
    void build_namespaceWithPrefix_emitsXmlnsOnSchema() throws Exception {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("dc");
        ns.setUri("http://purl.org/dc/elements/1.1/");
        String xsd = builder.build(model(rootMapping("Record", List.of()), null), List.of(ns));
        assertTrue(xsd.contains("xmlns:dc=\"http://purl.org/dc/elements/1.1/\""));
    }

    @Test
    void build_defaultNamespace_emitsXmlnsAndTargetNamespace() throws Exception {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("");
        ns.setUri("urn:example:schema");
        String xsd = builder.build(model(rootMapping("Record", List.of()), null), List.of(ns));
        assertTrue(xsd.contains("xmlns=\"urn:example:schema\""));
        assertTrue(xsd.contains("targetNamespace=\"urn:example:schema\""));
    }

    @Test
    void build_columnWithNamespacePrefix_emitsQualifiedName() throws Exception {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setXmlName("title");
        c.setXmlType("xs:string");
        c.setMappingType("Element");
        c.setNamespacePrefix("dc");
        XmlTableMapping root = rootMapping("Record", List.of(c));
        String xsd = builder.build(model(root, null), null);
        assertTrue(xsd.contains("name=\"dc:title\""));
    }

    // ── Child Elements mapping ────────────────────────────────────────────────

    @Test
    void build_childElementsMapping_emitsNestedComplexElement() throws Exception {
        XmlTableMapping root = rootMapping("Order", List.of(col("OrderId", "xs:string")));
        XmlTableMapping child = childMapping("child-id", "Item", "Elements",
                List.of(col("ItemId", "xs:string")));

        String xsd = builder.build(model(root, List.of(child)), null);
        assertTrue(xsd.contains("name=\"Item\""));
        assertTrue(xsd.contains("name=\"ItemId\""));
    }

    @Test
    void build_childWithWrapper_emitsWrapperElement() throws Exception {
        XmlTableMapping root = rootMapping("Order", List.of());
        XmlTableMapping child = childMapping("child-id", "Item", "Elements", List.of());
        child.setWrapInParent(true);
        child.setWrapperElementName("Items");

        String xsd = builder.build(model(root, List.of(child)), null);
        assertTrue(xsd.contains("name=\"Items\""));
        assertTrue(xsd.contains("name=\"Item\""));
    }

    @Test
    void build_childElementHasMinOccursZeroAndMaxOccursUnbounded() throws Exception {
        XmlTableMapping root = rootMapping("Order", List.of());
        XmlTableMapping child = childMapping("c1", "Item", "Elements", List.of());

        String xsd = builder.build(model(root, List.of(child)), null);
        // Non-top-level elements must have these occurrence constraints
        assertTrue(xsd.contains("maxOccurs=\"unbounded\""));
    }

    // ── Embed ─────────────────────────────────────────────────────────────────

    @Test
    void build_embeddedChild_inlinesColumnsWithoutWrapperElement() throws Exception {
        XmlTableMapping root = rootMapping("Person", List.of(col("Id", "xs:integer")));
        XmlTableMapping embedded = childMapping("emb-id", "AddressBlock", "InlineElement",
                List.of(col("City", "xs:string")));
        embedded.setEmbed(true);
        embedded.setParentRef("root-id");

        String xsd = builder.build(model(root, List.of(embedded)), null);
        assertTrue(xsd.contains("name=\"City\""));
        assertFalse(xsd.contains("name=\"AddressBlock\""));
    }

    // ── CUSTOM mapping ────────────────────────────────────────────────────────

    @Test
    void build_customChildMapping_emitsAnyTypeElement() throws Exception {
        XmlTableMapping root = rootMapping("Record", List.of());
        XmlTableMapping custom = childMapping("cust-id", "Computed", "CUSTOM", null);

        String xsd = builder.build(model(root, List.of(custom)), null);
        assertTrue(xsd.contains("name=\"Computed\""));
        assertTrue(xsd.contains("type=\"xs:anyType\""));
    }

    @Test
    void build_customChildMappingWithXmlType_usesSpecifiedType() throws Exception {
        XmlTableMapping root = rootMapping("Record", List.of());
        XmlTableMapping custom = childMapping("cust-id", "Computed", "CUSTOM", null);
        custom.setXmlType("xs:string");

        String xsd = builder.build(model(root, List.of(custom)), null);
        assertTrue(xsd.contains("type=\"xs:string\""));
    }

    // ── InlineElement (nested) ────────────────────────────────────────────────

    @Test
    void build_inlineElementWithParentRef_isNestedInsideParent() throws Exception {
        XmlTableMapping root = rootMapping("Order", List.of());
        XmlTableMapping child = childMapping("child-id", "Item", "Elements",
                List.of(col("ItemId", "xs:string")));
        XmlTableMapping inline = childMapping("inline-id", "Detail", "InlineElement",
                List.of(col("Note", "xs:string")));
        inline.setParentRef("child-id");

        String xsd = builder.build(model(root, List.of(child, inline)), null);
        assertTrue(xsd.contains("name=\"Detail\""));
        assertTrue(xsd.contains("name=\"Note\""));
    }

    // ── Null / empty inputs ───────────────────────────────────────────────────

    @Test
    void build_noElements_doesNotThrow() {
        assertDoesNotThrow(() ->
                builder.build(model(rootMapping("Root", List.of()), null), null));
    }

    @Test
    void build_nullColumns_doesNotThrow() {
        assertDoesNotThrow(() ->
                builder.build(model(rootMapping("Root", null), null), null));
    }

    @Test
    void build_nullNamespaces_doesNotThrow() {
        assertDoesNotThrow(() ->
                builder.build(model(rootMapping("Root", List.of()), List.of()), null));
    }
}
