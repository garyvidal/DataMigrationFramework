package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.service.generate.JsonSchemaGenerationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class JsonSchemaController {

    private final JsonSchemaGenerationService jsonSchemaGenerationService;

    /**
     * GET /v1/projects/{id}/generate/json/schema
     *
     * <p>Returns a JSON Schema (Draft-07) derived from the project's JSON document mapping.
     * No database connection is required — the schema is built from the mapping
     * metadata alone. Safe to call repeatedly — read-only.</p>
     */
    @GetMapping(value = "/v1/projects/{id}/generate/json/schema", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> schema(@PathVariable String id) {
        try {
            String jsonSchema = jsonSchemaGenerationService.generateSchema(id);
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(jsonSchema);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body("JSON Schema generation failed: " + e.getMessage());
        }
    }
}
