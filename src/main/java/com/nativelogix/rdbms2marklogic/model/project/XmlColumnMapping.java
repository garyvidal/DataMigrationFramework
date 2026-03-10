package com.nativelogix.rdbms2marklogic.model.project;

import lombok.Data;

@Data
public class XmlColumnMapping {
    /** Stable UUID — persists across renames. */
    String id;
    String sourceColumn;
    String xmlName;
    /** XSD type string, e.g. "xs:string", "xs:integer". */
    String xmlType;
    /** "Element", "ElementAttribute", or "CUSTOM". */
    String mappingType;
    String customFunction;
}
