package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.service.generate.XmlSchemaGenerationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class XmlSchemaController {

    private final XmlSchemaGenerationService xmlSchemaGenerationService;

    /**
     * GET /v1/projects/{id}/generate/schema
     *
     * <p>Returns an XSD derived from the project's XML document mapping.
     * No database connection is required — the schema is built from the mapping
     * metadata alone. Safe to call repeatedly — read-only.</p>
     */
    @GetMapping(value = "/v1/projects/{id}/generate/schema", produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<String> schema(@PathVariable String id) {
        try {
            String xsd = xmlSchemaGenerationService.generateSchema(id);
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_XML)
                    .body(xsd);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body("Schema generation failed: " + e.getMessage());
        }
    }
}
