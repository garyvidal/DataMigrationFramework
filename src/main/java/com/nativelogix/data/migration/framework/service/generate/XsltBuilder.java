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
 * Generates a MarkLogic-compatible XSLT 2.0 stylesheet from a project's {@link DocumentModel}.
 *
 * <p>The generated stylesheet is a working identity transform that preserves the document structure
 * produced by the migration framework.  Each mapped element and attribute gets its own named
 * template so developers can override individual fields without touching the rest of the transform.</p>
 *
 * <p>Output structure:</p>
 * <ul>
 *   <li>One {@code <xsl:template match="/">} entry point that invokes the root template</li>
 *   <li>One named template per table mapping (root, Elements, InlineElement) that rebuilds
 *       the element and recurses into its children</li>
 *   <li>Column-level element/attribute copy-of expressions — easy to swap for value-of or
 *       custom XPath expressions</li>
 *   <li>CUSTOM mappings emit a comment-placeholder that developers can fill in</li>
 * </ul>
 *
 * <p>No database connection is required — the stylesheet is built entirely from mapping metadata.</p>
 */
@Component
public class XsltBuilder {

    private static final String XSLT_NS = "http://www.w3.org/1999/XSL/Transform";
    private static final String XS_NS   = "http://www.w3.org/2001/XMLSchema";

    /**
     * Builds an XSLT string from the document model and optional namespace declarations.
     *
     * @param documentModel the project's XML mapping (root + child elements)
     * @param namespaces    project-level namespace declarations (may be null or empty)
     * @return pretty-printed XSLT 2.0 string
     */
    public String build(DocumentModel documentModel, List<XmlNamespace> namespaces)
            throws ParserConfigurationException, TransformerException {

        XmlTableMapping root = documentModel.getRoot();
        List<XmlTableMapping> elements = documentModel.getElements() != null
                ? documentModel.getElements() : Collections.emptyList();

        // Index inline mappings by parentRef
        Map<String, List<XmlTableMapping>> inlineByParent = elements.stream()
                .filter(m -> "InlineElement".equals(m.getMappingType()))
                .collect(Collectors.groupingBy(m -> m.getParentRef() != null ? m.getParentRef() : ""));

        // Root-level (non-inline) children
        List<XmlTableMapping> rootChildren = elements.stream()
                .filter(m -> !"InlineElement".equals(m.getMappingType()))
                .collect(Collectors.toList());

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

        // <xsl:stylesheet version="2.0" xmlns:xsl="..." [xmlns:prefix="uri"]*>
        Element stylesheet = doc.createElementNS(XSLT_NS, "xsl:stylesheet");
        stylesheet.setAttribute("version", "2.0");
        stylesheet.setAttribute("xmlns:xsl", XSLT_NS);
        stylesheet.setAttribute("xmlns:xs", XS_NS);
        if (namespaces != null) {
            for (XmlNamespace ns : namespaces) {
                if (ns.getPrefix() == null || ns.getUri() == null) continue;
                if (ns.getPrefix().isBlank()) {
                    stylesheet.setAttribute("xmlns", ns.getUri());
                } else {
                    stylesheet.setAttribute("xmlns:" + ns.getPrefix(), ns.getUri());
                }
            }
        }
        doc.appendChild(stylesheet);

        // <xsl:output method="xml" indent="yes" encoding="UTF-8"/>
        Element output = doc.createElementNS(XSLT_NS, "xsl:output");
        output.setAttribute("method", "xml");
        output.setAttribute("indent", "yes");
        output.setAttribute("encoding", "UTF-8");
        stylesheet.appendChild(output);

        appendBlankLine(doc, stylesheet);

        // Entry-point template: match="/"
        appendEntryTemplate(doc, stylesheet, root, namespaces);

        appendBlankLine(doc, stylesheet);

        // Named template for the root mapping
        appendMappingTemplate(doc, stylesheet, root, rootChildren, inlineByParent);

        // Named templates for every non-inline child mapping
        for (XmlTableMapping child : rootChildren) {
            appendBlankLine(doc, stylesheet);
            appendMappingTemplate(doc, stylesheet, child,
                    Collections.emptyList(), inlineByParent);
        }

        // Named templates for inline mappings (depth-first via recursion)
        for (XmlTableMapping inline : elements) {
            if ("InlineElement".equals(inline.getMappingType())) {
                appendBlankLine(doc, stylesheet);
                appendMappingTemplate(doc, stylesheet, inline,
                        Collections.emptyList(), inlineByParent);
            }
        }

        return serialize(doc);
    }

    // -------------------------------------------------------------------------
    // Template builders
    // -------------------------------------------------------------------------

    /**
     * Appends the {@code match="/"} entry template that creates the root element and
     * calls the root mapping's named template.
     */
    private void appendEntryTemplate(Document doc, Element stylesheet,
                                     XmlTableMapping root, List<XmlNamespace> namespaces) {

        Element tmpl = doc.createElementNS(XSLT_NS, "xsl:template");
        tmpl.setAttribute("match", "/");
        stylesheet.appendChild(tmpl);

        String rootElementName = resolvedName(root.getNamespacePrefix(), root.getXmlName());

        Element rootEl = doc.createElement(rootElementName);
        tmpl.appendChild(rootEl);

        // Emit namespace declarations on the output root element via xsl:namespace
        if (namespaces != null) {
            for (XmlNamespace ns : namespaces) {
                if (ns.getPrefix() == null || ns.getUri() == null) continue;
                Element nsEl = doc.createElementNS(XSLT_NS, "xsl:namespace");
                nsEl.setAttribute("name", ns.getPrefix().isBlank() ? "" : ns.getPrefix());
                nsEl.setAttribute("select", "'" + ns.getUri() + "'");
                rootEl.appendChild(nsEl);
            }
        }

        Element callRoot = doc.createElementNS(XSLT_NS, "xsl:call-template");
        callRoot.setAttribute("name", templateName(root));
        rootEl.appendChild(callRoot);
    }

    /**
     * Appends a named template for a single table mapping.  The template:
     * <ol>
     *   <li>Emits column-level elements (copy-of by default)</li>
     *   <li>Calls named templates for direct child table mappings</li>
     *   <li>Calls named templates for inline children of this mapping</li>
     * </ol>
     */
    private void appendMappingTemplate(Document doc, Element stylesheet,
                                       XmlTableMapping mapping,
                                       List<XmlTableMapping> directChildren,
                                       Map<String, List<XmlTableMapping>> inlineByParent) {

        if ("CUSTOM".equals(mapping.getMappingType())) {
            appendCustomPlaceholderTemplate(doc, stylesheet, mapping);
            return;
        }

        Element tmpl = doc.createElementNS(XSLT_NS, "xsl:template");
        tmpl.setAttribute("name", templateName(mapping));
        stylesheet.appendChild(tmpl);

        // Context: the current element matching this mapping's xmlName
        String elementName = resolvedName(mapping.getNamespacePrefix(), mapping.getXmlName());
        String contextPath = mapping.getXmlName() != null ? mapping.getXmlName() : "*";

        // xsl:param name="context" select="."  — caller may override for nested calls
        Element param = doc.createElementNS(XSLT_NS, "xsl:param");
        param.setAttribute("name", "context");
        param.setAttribute("select", ".");
        tmpl.appendChild(param);

        List<XmlColumnMapping> columns = mapping.getColumns() != null
                ? mapping.getColumns() : Collections.emptyList();

        List<XmlColumnMapping> attrCols = columns.stream()
                .filter(c -> "ElementAttribute".equals(c.getMappingType()))
                .collect(Collectors.toList());

        List<XmlColumnMapping> elementCols = columns.stream()
                .filter(c -> !"ElementAttribute".equals(c.getMappingType()))
                .collect(Collectors.toList());

        List<XmlTableMapping> inlineChildren = inlineByParent.getOrDefault(
                mapping.getId(), Collections.emptyList());

        boolean isRoot = "RootElement".equals(mapping.getMappingType());

        if (isRoot) {
            // Root template: iterate over root documents (context is already set at "/")
            Element forEachDoc = doc.createElementNS(XSLT_NS, "xsl:for-each");
            forEachDoc.setAttribute("select", "$context/" + contextPath);
            tmpl.appendChild(forEachDoc);

            appendColumnBody(doc, forEachDoc, elementCols, attrCols, directChildren, inlineChildren);
        } else {
            // Non-root: wrap output in the element, apply for-each over occurrences
            if (mapping.isWrapInParent() && mapping.getWrapperElementName() != null
                    && !mapping.getWrapperElementName().isBlank()) {

                Element wrapperEl = doc.createElement(mapping.getWrapperElementName());
                tmpl.appendChild(wrapperEl);

                Element forEach = doc.createElementNS(XSLT_NS, "xsl:for-each");
                forEach.setAttribute("select", "$context/" + contextPath);
                wrapperEl.appendChild(forEach);

                Element childEl = doc.createElement(elementName);
                forEach.appendChild(childEl);
                appendColumnBody(doc, childEl, elementCols, attrCols,
                        Collections.emptyList(), inlineChildren);

            } else if (mapping.isEmbed()) {
                // Embedded: emit columns directly, no wrapping element
                appendColumnBody(doc, tmpl, elementCols, attrCols,
                        Collections.emptyList(), inlineChildren);
            } else {
                Element forEach = doc.createElementNS(XSLT_NS, "xsl:for-each");
                forEach.setAttribute("select", "$context/" + contextPath);
                tmpl.appendChild(forEach);

                Element childEl = doc.createElement(elementName);
                forEach.appendChild(childEl);
                appendColumnBody(doc, childEl, elementCols, attrCols,
                        Collections.emptyList(), inlineChildren);
            }

            // Direct children (Elements-type) get their own template calls
            for (XmlTableMapping child : directChildren) {
                Element callChild = doc.createElementNS(XSLT_NS, "xsl:call-template");
                callChild.setAttribute("name", templateName(child));
                Element withParam = doc.createElementNS(XSLT_NS, "xsl:with-param");
                withParam.setAttribute("name", "context");
                withParam.setAttribute("select", ".");
                callChild.appendChild(withParam);
                tmpl.appendChild(callChild);
            }
        }
    }

    /**
     * Appends column elements, attributes, and inline child template calls into {@code parent}.
     */
    private void appendColumnBody(Document doc, Element parent,
                                  List<XmlColumnMapping> elementCols,
                                  List<XmlColumnMapping> attrCols,
                                  List<XmlTableMapping> directChildren,
                                  List<XmlTableMapping> inlineChildren) {

        // Attributes first
        for (XmlColumnMapping col : attrCols) {
            if (isSkippable(col)) continue;
            if ("CUSTOM".equals(col.getMappingType())) {
                parent.appendChild(doc.createComment(
                        " CUSTOM attribute: " + col.getXmlName() + " — add your XPath expression here "));
                continue;
            }
            Element attr = doc.createElementNS(XSLT_NS, "xsl:attribute");
            attr.setAttribute("name", resolvedName(col.getNamespacePrefix(), col.getXmlName()));
            Element valueOf = doc.createElementNS(XSLT_NS, "xsl:value-of");
            valueOf.setAttribute("select", "@" + col.getXmlName());
            attr.appendChild(valueOf);
            parent.appendChild(attr);
        }

        // Element children
        for (XmlColumnMapping col : elementCols) {
            if (isSkippable(col)) continue;
            if ("CUSTOM".equals(col.getMappingType())) {
                parent.appendChild(doc.createComment(
                        " CUSTOM element: " + col.getXmlName() + " — add your XPath expression here "));
                continue;
            }
            String colName = resolvedName(col.getNamespacePrefix(), col.getXmlName());
            Element copyOf = doc.createElementNS(XSLT_NS, "xsl:copy-of");
            copyOf.setAttribute("select", col.getXmlName());
            parent.appendChild(copyOf);
        }

        // Inline children — call their named template with current context
        for (XmlTableMapping inline : inlineChildren) {
            Element call = doc.createElementNS(XSLT_NS, "xsl:call-template");
            call.setAttribute("name", templateName(inline));
            Element withParam = doc.createElementNS(XSLT_NS, "xsl:with-param");
            withParam.setAttribute("name", "context");
            withParam.setAttribute("select", ".");
            call.appendChild(withParam);
            parent.appendChild(call);
        }

        // Direct child template calls (used for root-level template only)
        for (XmlTableMapping child : directChildren) {
            Element call = doc.createElementNS(XSLT_NS, "xsl:call-template");
            call.setAttribute("name", templateName(child));
            Element withParam = doc.createElementNS(XSLT_NS, "xsl:with-param");
            withParam.setAttribute("name", "context");
            withParam.setAttribute("select", ".");
            call.appendChild(withParam);
            parent.appendChild(call);
        }
    }

    /** Emits a comment-only named template for CUSTOM table mappings. */
    private void appendCustomPlaceholderTemplate(Document doc, Element stylesheet,
                                                 XmlTableMapping mapping) {
        Element tmpl = doc.createElementNS(XSLT_NS, "xsl:template");
        tmpl.setAttribute("name", templateName(mapping));
        Element param = doc.createElementNS(XSLT_NS, "xsl:param");
        param.setAttribute("name", "context");
        param.setAttribute("select", ".");
        tmpl.appendChild(param);
        tmpl.appendChild(doc.createComment(
                " CUSTOM mapping '" + mapping.getXmlName() + "' — implement transform logic here "));
        stylesheet.appendChild(tmpl);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Derives a safe XSLT template name from the mapping's table + xmlName.
     * Result is always a valid NCName.
     */
    private String templateName(XmlTableMapping mapping) {
        String table = mapping.getSourceTable() != null ? mapping.getSourceTable() : "unknown";
        String xml   = mapping.getXmlName()    != null ? mapping.getXmlName()    : "element";
        return "tpl-" + sanitize(table) + "-" + sanitize(xml);
    }

    private String sanitize(String s) {
        return s.replaceAll("[^A-Za-z0-9_\\-]", "_").toLowerCase();
    }

    private String resolvedName(String prefix, String localName) {
        if (prefix != null && !prefix.isBlank()) {
            return prefix + ":" + localName;
        }
        return localName != null ? localName : "";
    }

    private boolean isSkippable(XmlColumnMapping col) {
        return col.getXmlName() == null || col.getXmlName().isBlank();
    }

    private void appendBlankLine(Document doc, Element parent) {
        parent.appendChild(doc.createTextNode("\n"));
    }

    private String serialize(Document doc) throws TransformerException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.VERSION, "1.0");

        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }
}
