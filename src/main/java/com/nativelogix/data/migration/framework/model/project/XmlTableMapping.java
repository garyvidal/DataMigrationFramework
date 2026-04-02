package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

import java.util.List;

@Data
public class XmlTableMapping {
    /** Stable UUID — persists across renames. */
    String id;
    String sourceSchema;
    String sourceTable;
    String xmlName;
    /** "RootElement", "Elements", "InlineElement", or "CUSTOM". */
    String mappingType;
    boolean wrapInParent;
    String wrapperElementName;
    /** InlineElement only: id of the parent XmlTableMapping this is nested inside. */
    String parentRef;
    /** InlineElement only: when true, skip the wrapper element and embed columns directly into the parent. */
    boolean embed;
    /** When multiple FKs exist between parent and child, specifies the FK column to use for joining. */
    String joinColumn;
    /** CUSTOM only: JavaScript function body. */
    String customFunction;
    /** CUSTOM only: XSD return type, e.g. "xs:string". */
    String xmlType;
    /** Optional namespace prefix for this element, e.g. "dc". Must be declared in ProjectMapping.namespaces. */
    String namespacePrefix;
    List<XmlColumnMapping> columns;
}
