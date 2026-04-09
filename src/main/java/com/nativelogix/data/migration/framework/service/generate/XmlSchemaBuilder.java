package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.*;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Derives an XML Schema (XSD) from the project's {@link DocumentModel} mapping definitions.
 *
 * <p>No database connection is required — the schema is built entirely from the mapping
 * metadata (element names, XSD types, attributes, nesting relationships).</p>
 *
 * <p>Structure rules:</p>
 * <ul>
 *   <li>Root mapping → top-level {@code xs:element} with an inline {@code xs:complexType}</li>
 *   <li>{@code Elements} mappings → {@code xs:element} children in root's sequence; optionally
 *       wrapped in a parent element when {@code wrapInParent=true}</li>
 *   <li>{@code InlineElement} mappings → nested inside their parent's complex type at any depth</li>
 *   <li>Column with {@code mappingType=Element} → {@code xs:element} child</li>
 *   <li>Column with {@code mappingType=ElementAttribute} → {@code xs:attribute}</li>
 *   <li>Column {@code xmlType} is used as-is (e.g. {@code xs:string}, {@code xs:integer});
 *       defaults to {@code xs:string} when absent</li>
 *   <li>Namespace declarations from {@link ProjectMapping#getNamespaces()} are emitted on
 *       the {@code xs:schema} root element</li>
 * </ul>
 */
@Component
public class XmlSchemaBuilder {

    private static final String XS_NS = "http://www.w3.org/2001/XMLSchema";

    /**
     * Builds an XSD string from the document model and namespace declarations.
     *
     * @param documentModel the project's XML mapping (root + child elements)
     * @param namespaces    project-level namespace declarations (may be null or empty)
     * @return pretty-printed XSD string
     */
    public String build(DocumentModel documentModel, List<XmlNamespace> namespaces)
            throws ParserConfigurationException, TransformerException {

        XmlTableMapping root = documentModel.getRoot();
        List<XmlTableMapping> elements = documentModel.getElements() != null
                ? documentModel.getElements() : Collections.emptyList();

        // Index inline mappings by parentRef so we can nest them correctly
        Map<String, List<XmlTableMapping>> inlineByParent = elements.stream()
                .filter(m -> "InlineElement".equals(m.getMappingType()))
                .collect(Collectors.groupingBy(m -> m.getParentRef() != null ? m.getParentRef() : ""));

        // Root-level children (Elements) — excludes InlineElement mappings
        List<XmlTableMapping> rootChildren = elements.stream()
                .filter(m -> !"InlineElement".equals(m.getMappingType()))
                .collect(Collectors.toList());

        // Build namespace URI map: prefix → URI
        Map<String, String> nsMap = buildNsMap(namespaces);

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

        // <xs:schema xmlns:xs="..." [xmlns:prefix="uri"]*>
        Element schema = doc.createElementNS(XS_NS, "xs:schema");
        schema.setAttribute("xmlns:xs", XS_NS);
        schema.setAttribute("elementFormDefault", "qualified");
        if (namespaces != null) {
            for (XmlNamespace ns : namespaces) {
                if (ns.getPrefix() == null || ns.getUri() == null) continue;
                if (ns.getPrefix().isBlank()) {
                    schema.setAttribute("xmlns", ns.getUri());
                    schema.setAttribute("targetNamespace", ns.getUri());
                } else {
                    schema.setAttribute("xmlns:" + ns.getPrefix(), ns.getUri());
                }
            }
        }
        doc.appendChild(schema);

        // Top-level element for the root mapping
        Element rootEl = buildComplexElement(doc, schema, root, rootChildren, inlineByParent, nsMap, true);
        schema.appendChild(rootEl);

        return serialize(doc);
    }

    // -------------------------------------------------------------------------
    // Element builders
    // -------------------------------------------------------------------------

    /**
     * Builds an {@code xs:element} with an inline {@code xs:complexType} containing a sequence
     * of child elements (from columns and nested mappings) plus any attributes.
     */
    private Element buildComplexElement(Document doc, Element schema,
                                        XmlTableMapping mapping,
                                        List<XmlTableMapping> directChildren,
                                        Map<String, List<XmlTableMapping>> inlineByParent,
                                        Map<String, String> nsMap,
                                        boolean topLevel) {

        Element xsElement = doc.createElementNS(XS_NS, "xs:element");
        xsElement.setAttribute("name", resolvedName(mapping.getNamespacePrefix(), mapping.getXmlName()));
        if (!topLevel) {
            xsElement.setAttribute("minOccurs", "0");
            xsElement.setAttribute("maxOccurs", "unbounded");
        }

        List<XmlColumnMapping> columns = mapping.getColumns() != null
                ? mapping.getColumns() : Collections.emptyList();

        List<XmlColumnMapping> elementCols = columns.stream()
                .filter(c -> !"ElementAttribute".equals(c.getMappingType()))
                .collect(Collectors.toList());
        List<XmlColumnMapping> attrCols = columns.stream()
                .filter(c -> "ElementAttribute".equals(c.getMappingType()))
                .collect(Collectors.toList());

        // Inline children of this mapping
        List<XmlTableMapping> inlineChildren = inlineByParent.getOrDefault(mapping.getId(), Collections.emptyList());

        boolean hasContent = !elementCols.isEmpty() || !directChildren.isEmpty() || !inlineChildren.isEmpty();

        if (hasContent || !attrCols.isEmpty()) {
            Element complexType = doc.createElementNS(XS_NS, "xs:complexType");
            xsElement.appendChild(complexType);

            if (hasContent) {
                Element sequence = doc.createElementNS(XS_NS, "xs:sequence");
                complexType.appendChild(sequence);

                // Column-level elements first
                for (XmlColumnMapping col : elementCols) {
                    if (isSkippableColumn(col)) continue;
                    sequence.appendChild(buildColumnElement(doc, col));
                }

                // Root-level child mappings (Elements), respecting wrapInParent
                for (XmlTableMapping child : directChildren) {
                    appendChildMapping(doc, schema, sequence, child, inlineByParent, nsMap);
                }

                // Inline children (InlineElement) nested into this mapping
                for (XmlTableMapping inline : inlineChildren) {
                    appendChildMapping(doc, schema, sequence, inline, inlineByParent, nsMap);
                }
            }

            // Attributes
            for (XmlColumnMapping col : attrCols) {
                if (isSkippableColumn(col)) continue;
                complexType.appendChild(buildAttributeDecl(doc, col));
            }
        }

        return xsElement;
    }

    /**
     * Appends a child mapping to the given sequence, handling wrapInParent, embed, and CUSTOM.
     */
    private void appendChildMapping(Document doc, Element schema, Element sequence,
                                    XmlTableMapping child,
                                    Map<String, List<XmlTableMapping>> inlineByParent,
                                    Map<String, String> nsMap) {

        if ("CUSTOM".equals(child.getMappingType())) {
            // Emit a loosely-typed any element for CUSTOM mappings
            sequence.appendChild(buildAnyElement(doc, child.getXmlName(), child.getXmlType()));
            return;
        }

        if (child.isEmbed()) {
            // Embed: inline the column elements directly into the parent sequence
            if (child.getColumns() != null) {
                for (XmlColumnMapping col : child.getColumns()) {
                    if (isSkippableColumn(col)) continue;
                    if (!"ElementAttribute".equals(col.getMappingType())) {
                        sequence.appendChild(buildColumnElement(doc, col));
                    }
                }
            }
            // Recurse inline children of the embedded mapping
            for (XmlTableMapping nested : inlineByParent.getOrDefault(child.getId(), Collections.emptyList())) {
                appendChildMapping(doc, schema, sequence, nested, inlineByParent, nsMap);
            }
            return;
        }

        // Normal or wrapInParent
        if (child.isWrapInParent() && child.getWrapperElementName() != null && !child.getWrapperElementName().isBlank()) {
            // <xs:element name="wrapper" minOccurs="0">
            //   <xs:complexType><xs:sequence>
            //     <xs:element name="child" .../>
            //   </xs:sequence></xs:complexType>
            // </xs:element>
            Element wrapper = doc.createElementNS(XS_NS, "xs:element");
            wrapper.setAttribute("name", child.getWrapperElementName());
            wrapper.setAttribute("minOccurs", "0");
            Element wrapperType = doc.createElementNS(XS_NS, "xs:complexType");
            Element wrapperSeq = doc.createElementNS(XS_NS, "xs:sequence");
            wrapperType.appendChild(wrapperSeq);
            wrapper.appendChild(wrapperType);

            wrapperSeq.appendChild(buildComplexElement(doc, schema, child,
                    Collections.emptyList(), inlineByParent, nsMap, false));
            sequence.appendChild(wrapper);
        } else {
            sequence.appendChild(buildComplexElement(doc, schema, child,
                    Collections.emptyList(), inlineByParent, nsMap, false));
        }
    }

    /** Builds an {@code xs:element} for a single column. */
    private Element buildColumnElement(Document doc, XmlColumnMapping col) {
        Element el = doc.createElementNS(XS_NS, "xs:element");
        el.setAttribute("name", resolvedName(col.getNamespacePrefix(), col.getXmlName()));
        el.setAttribute("type", xsType(col.getXmlType()));
        el.setAttribute("minOccurs", "0");
        el.setAttribute("maxOccurs", "1");
        return el;
    }

    /** Builds an {@code xs:attribute} for a column mapped as an attribute. */
    private Element buildAttributeDecl(Document doc, XmlColumnMapping col) {
        Element attr = doc.createElementNS(XS_NS, "xs:attribute");
        attr.setAttribute("name", resolvedName(col.getNamespacePrefix(), col.getXmlName()));
        attr.setAttribute("type", xsType(col.getXmlType()));
        attr.setAttribute("use", "optional");
        return attr;
    }

    /**
     * Builds a loosely-typed placeholder element for CUSTOM mappings.
     * Uses the declared {@code xmlType} if present, otherwise {@code xs:anyType}.
     */
    private Element buildAnyElement(Document doc, String name, String xmlType) {
        Element el = doc.createElementNS(XS_NS, "xs:element");
        el.setAttribute("name", name != null ? name : "custom");
        el.setAttribute("type", xmlType != null && !xmlType.isBlank() ? xmlType : "xs:anyType");
        el.setAttribute("minOccurs", "0");
        el.setAttribute("maxOccurs", "unbounded");
        return el;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** True if the column should be skipped (no name or is a nameless CUSTOM with no function). */
    private boolean isSkippableColumn(XmlColumnMapping col) {
        return col.getXmlName() == null || col.getXmlName().isBlank();
    }

    /** Returns {@code prefix:localName} if prefix is set, otherwise {@code localName}. */
    private String resolvedName(String prefix, String localName) {
        if (prefix != null && !prefix.isBlank()) {
            return prefix + ":" + localName;
        }
        return localName;
    }

    /** Returns the XSD type string, defaulting to {@code xs:string}. */
    private String xsType(String xmlType) {
        return (xmlType != null && !xmlType.isBlank()) ? xmlType : "xs:string";
    }

    private Map<String, String> buildNsMap(List<XmlNamespace> namespaces) {
        if (namespaces == null || namespaces.isEmpty()) return Map.of();
        Map<String, String> map = new LinkedHashMap<>();
        for (XmlNamespace ns : namespaces) {
            if (ns.getPrefix() != null && ns.getUri() != null) {
                map.put(ns.getPrefix(), ns.getUri());
            }
        }
        return map;
    }

    private String serialize(Document doc) throws TransformerException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }
}
