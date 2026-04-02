package com.nativelogix.rdbms2marklogic.model.project;

import lombok.Data;

/**
 * A single XML namespace declaration: a prefix (e.g. "xs") and its URI
 * (e.g. "http://www.w3.org/2001/XMLSchema").
 *
 * <p>Namespaces are stored in {@link ProjectMapping#getNamespaces()} and declared
 * on the root element of every generated XML document as {@code xmlns:prefix="uri"}.</p>
 */
@Data
public class XmlNamespace {
    /** Namespace prefix, e.g. {@code "xs"}, {@code "dc"}, {@code "my"}. */
    String prefix;
    /** Namespace URI, e.g. {@code "http://www.w3.org/2001/XMLSchema"}. */
    String uri;
}
