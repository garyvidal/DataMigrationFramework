package com.nativelogix.data.migration.framework.service.generate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nativelogix.data.migration.framework.model.project.JsonColumnMapping;
import com.nativelogix.data.migration.framework.model.project.JsonDocumentModel;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Derives a JSON Schema (Draft-07) from the project's {@link JsonDocumentModel} mapping definitions.
 *
 * <p>No database connection is required — the schema is built entirely from the mapping
 * metadata (property names, JSON types, nesting relationships).</p>
 *
 * <p>Structure rules:</p>
 * <ul>
 *   <li>RootObject mapping → top-level {@code object} schema with {@code properties}</li>
 *   <li>{@code Array} mappings → {@code array} property in the parent object whose {@code items}
 *       is an {@code object} schema; optionally wrapped in a parent key when {@code wrapInParent=true}</li>
 *   <li>{@code InlineObject} mappings → nested {@code object} inside their parent's {@code properties}
 *       at any depth</li>
 *   <li>Column with {@code mappingType=Property} → scalar property with the declared {@code jsonType}</li>
 *   <li>Column with {@code mappingType=CUSTOM} → loosely-typed property ({@code {}})</li>
 *   <li>{@code embed=true} on an InlineObject → the inline object's properties are merged directly
 *       into the parent object's {@code properties} rather than nested under a key</li>
 * </ul>
 */
@Component
public class JsonSchemaBuilder {

    private static final String DRAFT_07 = "http://json-schema.org/draft-07/schema#";
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Builds a JSON Schema string from the JSON document model.
     *
     * @param documentModel the project's JSON mapping (root + child elements)
     * @return pretty-printed JSON Schema string
     */
    public String build(JsonDocumentModel documentModel) throws Exception {
        JsonTableMapping root = documentModel.getRoot();
        List<JsonTableMapping> elements = documentModel.getElements() != null
                ? documentModel.getElements() : Collections.emptyList();

        // Index InlineObject mappings by parentRef
        Map<String, List<JsonTableMapping>> inlineByParent = elements.stream()
                .filter(m -> "InlineObject".equals(m.getMappingType()))
                .collect(Collectors.groupingBy(m -> m.getParentRef() != null ? m.getParentRef() : ""));

        // Root-level Array children (excludes InlineObject)
        List<JsonTableMapping> rootChildren = elements.stream()
                .filter(m -> !"InlineObject".equals(m.getMappingType()))
                .collect(Collectors.toList());

        // Build the inner object schema for the root mapping
        ObjectNode rootObjSchema = mapper.createObjectNode();
        rootObjSchema.put("type", "object");
        ObjectNode rootObjProps = mapper.createObjectNode();
        buildObjectProperties(rootObjProps, root, rootChildren, inlineByParent);
        rootObjSchema.set("properties", rootObjProps);

        // Wrap under the root element name
        ObjectNode topProperties = mapper.createObjectNode();
        topProperties.set(root.getJsonName(), rootObjSchema);

        ObjectNode schema = mapper.createObjectNode();
        schema.put("$schema", DRAFT_07);
        schema.put("type", "object");
        schema.set("properties", topProperties);

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
    }

    // -------------------------------------------------------------------------
    // Object builders
    // -------------------------------------------------------------------------

    /**
     * Populates {@code properties} with entries from the mapping's columns and child mappings.
     */
    private void buildObjectProperties(ObjectNode properties,
                                       JsonTableMapping mapping,
                                       List<JsonTableMapping> directChildren,
                                       Map<String, List<JsonTableMapping>> inlineByParent) {

        List<JsonColumnMapping> columns = mapping.getColumns() != null
                ? mapping.getColumns() : Collections.emptyList();

        // Column properties
        for (JsonColumnMapping col : columns) {
            if (isSkippableColumn(col)) continue;
            if ("CUSTOM".equals(col.getMappingType())) {
                properties.set(col.getJsonKey(), mapper.createObjectNode());
            } else {
                ObjectNode prop = mapper.createObjectNode();
                prop.put("type", jsonType(col.getJsonType()));
                properties.set(col.getJsonKey(), prop);
            }
        }

        // Direct Array children
        for (JsonTableMapping child : directChildren) {
            appendChildMapping(properties, child, inlineByParent);
        }

        // InlineObject children of this mapping
        for (JsonTableMapping inline : inlineByParent.getOrDefault(mapping.getId(), Collections.emptyList())) {
            appendChildMapping(properties, inline, inlineByParent);
        }
    }

    /**
     * Appends a child mapping into the parent's {@code properties}, handling embed and Array types.
     */
    private void appendChildMapping(ObjectNode parentProperties,
                                    JsonTableMapping child,
                                    Map<String, List<JsonTableMapping>> inlineByParent) {

        if (child.isEmbed()) {
            // Embed: merge properties directly into the parent object
            ObjectNode embeddedProps = mapper.createObjectNode();
            buildObjectProperties(embeddedProps, child, Collections.emptyList(), inlineByParent);
            embeddedProps.fields().forEachRemaining(e -> parentProperties.set(e.getKey(), e.getValue()));
            return;
        }

        String key = child.getJsonName();
        if (key == null || key.isBlank()) return;

        if ("Array".equals(child.getMappingType())) {
            // Array: { "type": "array", "items": { "type": "object", "properties": {...} } }
            ObjectNode arraySchema = mapper.createObjectNode();
            arraySchema.put("type", "array");

            ObjectNode itemSchema = mapper.createObjectNode();
            itemSchema.put("type", "object");
            ObjectNode itemProps = mapper.createObjectNode();
            buildObjectProperties(itemProps, child, Collections.emptyList(), inlineByParent);
            itemSchema.set("properties", itemProps);
            arraySchema.set("items", itemSchema);

            parentProperties.set(key, arraySchema);
        } else {
            // InlineObject: { "type": "object", "properties": {...} }
            ObjectNode objSchema = mapper.createObjectNode();
            objSchema.put("type", "object");
            ObjectNode objProps = mapper.createObjectNode();
            buildObjectProperties(objProps, child, Collections.emptyList(), inlineByParent);
            objSchema.set("properties", objProps);
            parentProperties.set(key, objSchema);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** True if the column has no key and should be skipped. */
    private boolean isSkippableColumn(JsonColumnMapping col) {
        return col.getJsonKey() == null || col.getJsonKey().isBlank();
    }

    /** Maps JSON mapping types to JSON Schema type strings, defaulting to {@code string}. */
    private String jsonType(String jsonType) {
        if (jsonType == null) return "string";
        return switch (jsonType.toLowerCase()) {
            case "number"  -> "number";
            case "boolean" -> "boolean";
            default        -> "string";
        };
    }
}
