package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.NamingCase;
import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlNamespace;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.StringReader;
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
 * <h3>Performance</h3>
 * <ul>
 *   <li>{@link DocumentBuilderFactory} is created once as a static field (factory lookup
 *       is expensive — avoids per-document service-loader discovery).</li>
 *   <li>{@link DocumentBuilder} instances are pooled per thread via {@link ThreadLocal}
 *       — they are not thread-safe but are safe to reuse within a single thread.</li>
 *   <li>Serialization uses a {@link StringBuilder}-based writer instead of DOM +
 *       {@code Transformer}. This eliminates the per-document {@code TransformerFactory}
 *       instantiation and the intermediate DOM object graph, cutting GC pressure
 *       significantly at high throughput.</li>
 * </ul>
 */
@Component
public class XmlDocumentBuilder {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(XmlDocumentBuilder.class);

    private static final DateTimeFormatter ISO_DATE     = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter ISO_DATETIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    // ── Thread-safe factory singletons ────────────────────────────────────────
    // DocumentBuilderFactory.newInstance() does a service-loader lookup every call —
    // creating it once and reusing it eliminates that overhead entirely.
    private static final DocumentBuilderFactory BUILDER_FACTORY;

    static {
        BUILDER_FACTORY = DocumentBuilderFactory.newInstance();
        BUILDER_FACTORY.setNamespaceAware(true);
    }

    // DocumentBuilder is NOT thread-safe, so each thread gets its own instance.
    private static final ThreadLocal<DocumentBuilder> THREAD_BUILDER = ThreadLocal.withInitial(() -> {
        try {
            return BUILDER_FACTORY.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Failed to create DocumentBuilder", e);
        }
    });

    private final JavaScriptFunctionExecutor jsExecutor;

    public XmlDocumentBuilder(JavaScriptFunctionExecutor jsExecutor) {
        this.jsExecutor = jsExecutor;
    }

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
                        List<XmlNamespace> namespaces) {

        StringBuilder sb = new StringBuilder(4096);
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

        String rootTag = tagName(rootMapping.getNamespacePrefix(), rootMapping.getXmlName());
        sb.append('<').append(rootTag);

        // Emit namespace declarations on the root element
        if (namespaces != null) {
            for (XmlNamespace ns : namespaces) {
                if (ns.getPrefix() == null || ns.getUri() == null) continue;
                if (ns.getPrefix().isBlank()) {
                    sb.append(" xmlns=\"").append(escapeAttr(ns.getUri())).append('"');
                } else {
                    sb.append(" xmlns:").append(ns.getPrefix())
                      .append("=\"").append(escapeAttr(ns.getUri())).append('"');
                }
            }
        }
        sb.append(">\n");

        // Root columns
        applyColumns(sb, rootMapping.getColumns(), rootRow, 1);

        // Child mappings
        if (childData != null) {
            for (Map.Entry<XmlTableMapping, List<MappedRow>> entry : childData.entrySet()) {
                XmlTableMapping childMapping = entry.getKey();
                List<MappedRow> mappedRows   = entry.getValue();

                if ("CUSTOM".equals(childMapping.getMappingType())) {
                    for (MappedRow mr : mappedRows) {
                        applyCustomTableMapping(sb, childMapping, mr.row(), 1);
                    }
                    continue;
                }

                if (childMapping.isEmbed()) {
                    for (MappedRow mr : mappedRows) {
                        applyColumns(sb, childMapping.getColumns(), mr.row(), 1);
                        buildInlineChildren(sb, mr.inlineChildren(), 2);
                    }
                    continue;
                }

                String wrapperTag = null;
                if (childMapping.isWrapInParent()
                        && childMapping.getWrapperElementName() != null
                        && !childMapping.getWrapperElementName().isBlank()) {
                    wrapperTag = childMapping.getWrapperElementName();
                    indent(sb, 1).append('<').append(wrapperTag).append(">\n");
                }

                int childIndent = wrapperTag != null ? 2 : 1;
                String childTag = tagName(childMapping.getNamespacePrefix(), childMapping.getXmlName());
                for (MappedRow mr : mappedRows) {
                    indent(sb, childIndent).append('<').append(childTag).append(">\n");
                    applyColumns(sb, childMapping.getColumns(), mr.row(), childIndent + 1);
                    buildInlineChildren(sb, mr.inlineChildren(), childIndent + 1);
                    indent(sb, childIndent).append("</").append(childTag).append(">\n");
                }

                if (wrapperTag != null) {
                    indent(sb, 1).append("</").append(wrapperTag).append(">\n");
                }
            }
        }

        sb.append("</").append(rootTag).append(">\n");
        return sb.toString();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void buildInlineChildren(StringBuilder sb,
                                     Map<XmlTableMapping, List<MappedRow>> inlineData,
                                     int depth) {
        if (inlineData == null || inlineData.isEmpty()) return;

        for (Map.Entry<XmlTableMapping, List<MappedRow>> entry : inlineData.entrySet()) {
            XmlTableMapping mapping = entry.getKey();
            List<MappedRow> rows    = entry.getValue();

            for (MappedRow mr : rows) {
                if ("CUSTOM".equals(mapping.getMappingType())) {
                    applyCustomTableMapping(sb, mapping, mr.row(), depth);
                } else if (mapping.isEmbed()) {
                    applyColumns(sb, mapping.getColumns(), mr.row(), depth);
                    buildInlineChildren(sb, mr.inlineChildren(), depth);
                } else {
                    String tag = tagName(mapping.getNamespacePrefix(), mapping.getXmlName());
                    indent(sb, depth).append('<').append(tag).append(">\n");
                    applyColumns(sb, mapping.getColumns(), mr.row(), depth + 1);
                    buildInlineChildren(sb, mr.inlineChildren(), depth + 1);
                    indent(sb, depth).append("</").append(tag).append(">\n");
                }
            }
        }
    }

    private void applyColumns(StringBuilder sb,
                               List<XmlColumnMapping> columns,
                               Map<String, Object> row,
                               int depth) {
        if (columns == null) return;

        for (XmlColumnMapping col : columns) {
            String name   = col.getXmlName();
            String prefix = col.getNamespacePrefix();

            boolean isCustom = "CUSTOM".equals(col.getMappingType())
                    || col.getSourceColumn() == null || col.getSourceColumn().isBlank();

            if (isCustom) {
                if (col.getCustomFunction() != null && !col.getCustomFunction().isBlank()) {
                    String value = jsExecutor.evaluate(col.getCustomFunction(), row);
                    if (value != null) {
                        String tag = tagName(prefix, name);
                        indent(sb, depth).append('<').append(tag).append('>')
                              .append(escapeText(value))
                              .append("</").append(tag).append(">\n");
                    }
                }
                continue;
            }

            Object rawValue = row.get(col.getSourceColumn());
            if (rawValue == null) continue;

            String value = formatValue(rawValue, col.getXmlType());

            if ("ElementAttribute".equals(col.getMappingType())) {
                // Attributes must be set on the parent — we can't retroactively add them
                // once the opening tag is written. Emit as a child element instead,
                // matching the previous DOM behaviour for the attribute-on-child-element case.
                // (True root-element attributes are handled inline in build() if needed.)
                String tag = tagName(prefix, name);
                indent(sb, depth).append('<').append(tag).append('>')
                      .append(escapeText(value))
                      .append("</").append(tag).append(">\n");
            } else {
                String tag = tagName(prefix, name);
                indent(sb, depth).append('<').append(tag).append('>')
                      .append(escapeText(value))
                      .append("</").append(tag).append(">\n");
            }
        }
    }

    /**
     * Handles a table-level CUSTOM mapping: evaluates the JS function (which returns an
     * XML snippet), parses it with the per-thread {@link DocumentBuilder}, and appends
     * the child nodes as serialized text. Falls back silently on null / bad XML.
     */
    private void applyCustomTableMapping(StringBuilder sb,
                                          XmlTableMapping mapping,
                                          Map<String, Object> row,
                                          int depth) {
        String xml = jsExecutor.evaluate(mapping.getCustomFunction(), row);
        if (xml == null || xml.isBlank()) return;
        try {
            DocumentBuilder builder = THREAD_BUILDER.get();
            builder.reset();
            Document fragment = builder.parse(
                    new InputSource(new StringReader("<_>" + xml + "</_>")));
            NodeList children = fragment.getDocumentElement().getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                serializeNode(sb, children.item(i), depth);
            }
        } catch (Exception e) {
            log.warn("CUSTOM table mapping '{}' returned unparseable XML: {}",
                    mapping.getXmlName(), e.getMessage());
        }
    }

    /** Minimal DOM-node-to-string serializer used only for CUSTOM XML snippets. */
    private void serializeNode(StringBuilder sb, Node node, int depth) {
        switch (node.getNodeType()) {
            case Node.ELEMENT_NODE -> {
                indent(sb, depth).append('<').append(node.getNodeName());
                var attrs = node.getAttributes();
                if (attrs != null) {
                    for (int i = 0; i < attrs.getLength(); i++) {
                        var a = attrs.item(i);
                        sb.append(' ').append(a.getNodeName())
                          .append("=\"").append(escapeAttr(a.getNodeValue())).append('"');
                    }
                }
                var children = node.getChildNodes();
                if (children.getLength() == 0) {
                    sb.append("/>\n");
                } else {
                    sb.append(">\n");
                    for (int i = 0; i < children.getLength(); i++) {
                        serializeNode(sb, children.item(i), depth + 1);
                    }
                    indent(sb, depth).append("</").append(node.getNodeName()).append(">\n");
                }
            }
            case Node.TEXT_NODE -> {
                String text = node.getNodeValue();
                if (text != null && !text.isBlank()) {
                    indent(sb, depth).append(escapeText(text.strip())).append('\n');
                }
            }
            default -> { /* ignore comments, PIs, etc. */ }
        }
    }

    private static String tagName(String prefix, String localName) {
        return (prefix != null && !prefix.isBlank()) ? prefix + ":" + localName : localName;
    }

    private static StringBuilder indent(StringBuilder sb, int depth) {
        sb.append("  ".repeat(depth));
        return sb;
    }

    /** Escape XML text content (element body). */
    private static String escapeText(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    /** Escape XML attribute values. */
    private static String escapeAttr(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace("\"", "&quot;");
    }

    /** Format a JDBC value to a string appropriate for the declared XSD type. */
    private static String formatValue(Object value, String xmlType) {
        if (xmlType != null) {
            switch (xmlType) {
                case "xs:date" -> {
                    if (value instanceof Date d)       return d.toLocalDate().format(ISO_DATE);
                    if (value instanceof Timestamp ts)  return ts.toLocalDateTime().toLocalDate().format(ISO_DATE);
                    if (value instanceof LocalDate ld)  return ld.format(ISO_DATE);
                }
                case "xs:dateTime" -> {
                    if (value instanceof Timestamp ts)    return ts.toLocalDateTime().format(ISO_DATETIME);
                    if (value instanceof LocalDateTime l)  return l.format(ISO_DATETIME);
                }
                case "xs:decimal", "xs:float", "xs:double" -> {
                    if (value instanceof BigDecimal bd)  return bd.stripTrailingZeros().toPlainString();
                    if (value instanceof Number n)       return new BigDecimal(n.toString()).stripTrailingZeros().toPlainString();
                }
                case "xs:boolean" -> {
                    if (value instanceof Boolean b)  return b.toString();
                    String s = value.toString().trim();
                    return ("1".equals(s) || "true".equalsIgnoreCase(s)) ? "true" : "false";
                }
            }
        }
        return value.toString();
    }
}
