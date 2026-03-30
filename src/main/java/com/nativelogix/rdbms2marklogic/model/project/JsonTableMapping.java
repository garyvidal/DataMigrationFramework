package com.nativelogix.rdbms2marklogic.model.project;

import lombok.Data;

import java.util.List;

@Data
public class JsonTableMapping {
    /** Stable UUID — persists across renames. */
    String id;
    String sourceSchema;
    String sourceTable;
    /** Key name used in the parent JSON object. */
    String jsonName;
    /** "RootObject", "Array", or "InlineObject". */
    String mappingType;
    /** InlineObject only: id of the parent JsonTableMapping this is nested inside. */
    String parentRef;
    /** InlineObject only: when true, skip the wrapper key and embed properties directly into the parent object. */
    boolean embed;
    /** When multiple FKs exist between parent and child, specifies the FK column to use for joining. */
    String joinColumn;
    List<JsonColumnMapping> columns;
}
