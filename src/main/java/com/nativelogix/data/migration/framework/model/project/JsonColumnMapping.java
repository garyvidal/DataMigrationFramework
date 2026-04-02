package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

@Data
public class JsonColumnMapping {
    /** Stable UUID — persists across renames. */
    String id;
    String sourceColumn;
    /** Property name in the JSON output. */
    String jsonKey;
    /** JSON scalar type: "string", "number", or "boolean". */
    String jsonType;
    /** "Property" or "CUSTOM". */
    String mappingType;
    String customFunction;
}
