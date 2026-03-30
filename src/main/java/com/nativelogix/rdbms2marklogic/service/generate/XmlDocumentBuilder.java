
package com.nativelogix.rdbms2marklogic.service.generate;

import com.nativelogix.rdbms2marklogic.model.project.NamingCase;
import com.nativelogix.rdbms2marklogic.model.project.XmlColumnMapping;
import com.nativelogix.rdbms2marklogic.model.project.XmlNamespace;
import com.nativelogix.rdbms2marklogic.model.project.XmlTableMapping;
import com.nativelogix.rdbms2marklogic.util.CaseConverter;
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
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Builds a formatted XML string from a root row and its related child rows,
 * driven by the project's {@link XmlTableMapping} definitions.
 *
 * <p>Child data is supplied as {@link MappedRow} instances, each carrying the raw row
 * plus the recursively-queried inline children for that row. This allows
 * {@code InlineElement} mappings to be nested inside the correct parent element at
 * any depth, matching the structure shown in the UI XML Preview.</p>
 *
 * <p>If the project defines XML namespaces, all {@code xmlns:prefix="uri"} declarations
 * are emitted on the root element, and any mapping whose {@code namespacePrefix} is set
 * will produce prefixed element/attribute names (e.g. {@code <dc:title>}).</p>
 */
@Component
public class XmlDocumentBuilder {

    private static final String XMLNS_NS = "http://www.w3.org/2000/xmlns/";
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter ISO_DATETIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /**
     * A single queried row together with any inline children that must be
     * nested inside it in the XML output.
     *
     * @param row            column name → JDBC value for this row
     * @param inlineChildren InlineElement mappings keyed to their own MappedRows,
     *                       to be rendered as child XML elements of this row's element
     */
    public record MappedRow(
            Map<String, Object> row,
            Map<XmlTableMapping, List<MappedRow>> inlineChildren) {}

    /**
     * Builds a single XML document string for one root row.
     *
     * @param rootMapping  the root table mapping (mappingType = "RootElement")
     * @param rootRow      column name → value map for the root row
     * @param childData    root-level child mappings → their MappedRows (with inline children attached)
     * @param casing       naming convention to apply to xmlName values (null = use as-is)
     * @param namespaces   project-level namespace declarations (may be null or empty)
     * @return pretty-printed XML string
     */
    public String build(XmlTableMapping rootMapping,
                        Map<String, Object> rootRow,
                        Map<XmlTableMapping, List<MappedRow>> childData,
                        NamingCase casing,
                        List<XmlNamespace> namespaces) throws ParserConfigurationException, TransformerException {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        Document doc = factory.newDocumentBuilder().newDocument();

        // Build namespace URI lookup: prefix → uri
        Map<String, String> nsMap = buildNsMap(namespaces);

        Element rootEl = createElement(doc, rootMapping.getNamespacePrefix(), rootMapping.getXmlName(), nsMap);
        doc.appendChild(rootEl);

        // Declare all project namespaces on the root element
        if (namespaces != null) {
            for (XmlNamespace ns : namespaces) {
                if (ns.getPrefix() == null || ns.getUri() == null) continue;
                if (ns.getPrefix().isBlank()) {
                    // Default namespace: xmlns="uri"
                    rootEl.setAttributeNS(XMLNS_NS, "xmlns", ns.getUri());
                } else {
                    // Prefixed namespace: xmlns:prefix="uri"
                    rootEl.setAttributeNS(XMLNS_NS, "xmlns:" + ns.getPrefix(), ns.getUri());
                }
            }
        }

        applyColumns(doc, rootEl, rootMapping.getColumns(), rootRow, nsMap);

        if (childData != null) {
            for (Map.Entry<XmlTableMapping, List<MappedRow>> entry : childData.entrySet()) {
                XmlTableMapping childMapping = entry.getKey();
                List<MappedRow> mappedRows = entry.getValue();

                Element parent = rootEl;

                if (childMapping.isWrapInParent() && childMapping.getWrapperElementName() != null && !childMapping.getWrapperElementName().isBlank()) {
                    Element wrapper = doc.createElement(childMapping.getWrapperElementName());
                    rootEl.appendChild(wrapper);
                    parent = wrapper;
                }

                for (MappedRow mr : mappedRows) {
                    Element childEl = createElement(doc, childMapping.getNamespacePrefix(), childMapping.getXmlName(), nsMap);
                    applyColumns(doc, childEl, childMapping.getColumns(), mr.row(), nsMap);
                    buildInlineChildren(doc, childEl, mr.inlineChildren(), nsMap);
                    parent.appendChild(childEl);
                }
            }
        }

        return serialize(doc);
    }

    // -------------------------------------------------------------------------

    /**
     * Recursively renders inline children into {@code parentEl}.
     * Each inline mapping produces its own XML element nested inside the parent.
     */
    private void buildInlineChildren(Document doc, Element parentEl,
                                     Map<XmlTableMapping, List<MappedRow>> inlineData,
                                     Map<String, String> nsMap) {
        if (inlineData == null || inlineData.isEmpty()) return;

        for (Map.Entry<XmlTableMapping, List<MappedRow>> entry : inlineData.entrySet()) {
            XmlTableMapping mapping = entry.getKey();
            List<MappedRow> rows = entry.getValue();

            for (MappedRow mr : rows) {
                Element inlineEl = createElement(doc, mapping.getNamespacePrefix(), mapping.getXmlName(), nsMap);
                applyColumns(doc, inlineEl, mapping.getColumns(), mr.row(), nsMap);
                buildInlineChildren(doc, inlineEl, mr.inlineChildren(), nsMap);
                parentEl.appendChild(inlineEl);
            }
        }
    }

    private void applyColumns(Document doc, Element parent,
                              List<XmlColumnMapping> columns,
                              Map<String, Object> row,
                              Map<String, String> nsMap) {
        if (columns == null) return;

        for (XmlColumnMapping col : columns) {
            if ("CUSTOM".equals(col.getMappingType())) continue;  // phase 2

            Object rawValue = row.get(col.getSourceColumn());
            if (rawValue == null) continue;  // omit null values

            String value = formatValue(rawValue, col.getXmlType());
            String name  = col.getXmlName();
            String prefix = col.getNamespacePrefix();

            if ("ElementAttribute".equals(col.getMappingType())) {
                if (prefix != null && !prefix.isBlank()) {
                    String uri = nsMap.get(prefix);
                    if (uri != null) {
                        parent.setAttributeNS(uri, prefix + ":" + name, value);
                    } else {
                        parent.setAttribute(name, value);
                    }
                } else {
                    parent.setAttribute(name, value);
                }
            } else {
                // Default: Element
                Element el = createElement(doc, prefix, name, nsMap);
                el.setTextContent(value);
                parent.appendChild(el);
            }
        }
    }

    /**
     * Creates an element, using the namespace URI if a known prefix is provided.
     * Falls back to a plain element if prefix is null/blank or not in nsMap.
     */
    private Element createElement(Document doc, String prefix, String localName, Map<String, String> nsMap) {
        if (prefix != null && !prefix.isBlank()) {
            String uri = nsMap.get(prefix);
            if (uri != null) {
                return doc.createElementNS(uri, prefix + ":" + localName);
            }
        }
        return doc.createElement(localName);
    }

    /** Build a prefix → URI lookup from the namespace list. */
    private Map<String, String> buildNsMap(List<XmlNamespace> namespaces) {
        if (namespaces == null || namespaces.isEmpty()) return Map.of();
        Map<String, String> map = new java.util.LinkedHashMap<>();
        for (XmlNamespace ns : namespaces) {
            if (ns.getPrefix() != null && ns.getUri() != null) {
                map.put(ns.getPrefix(), ns.getUri());
            }
        }
        return map;
    }

    /** Format a JDBC value to a string appropriate for the declared XSD type. */
    private String formatValue(Object value, String xmlType) {
        if (xmlType != null) {
            switch (xmlType) {
                case "xs:date" -> {
                    if (value instanceof Date d)      return d.toLocalDate().format(ISO_DATE);
                    if (value instanceof Timestamp ts) return ts.toLocalDateTime().toLocalDate().format(ISO_DATE);
                    if (value instanceof LocalDate ld) return ld.format(ISO_DATE);
                }
                case "xs:dateTime" -> {
                    if (value instanceof Timestamp ts)   return ts.toLocalDateTime().format(ISO_DATETIME);
                    if (value instanceof LocalDateTime l) return l.format(ISO_DATETIME);
                }
                case "xs:decimal", "xs:float", "xs:double" -> {
                    if (value instanceof BigDecimal bd) return bd.stripTrailingZeros().toPlainString();
                    if (value instanceof Number n)      return new BigDecimal(n.toString()).stripTrailingZeros().toPlainString();
                }
                case "xs:boolean" -> {
                    if (value instanceof Boolean b) return b.toString();
                    String s = value.toString().trim();
                    return ("1".equals(s) || "true".equalsIgnoreCase(s)) ? "true" : "false";
                }
            }
        }
        return value.toString();
    }

    private String serialize(Document doc) throws TransformerException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");

        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }
}
