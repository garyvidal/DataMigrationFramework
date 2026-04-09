package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.service.generate.XsltGenerationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class XsltController {

    private final XsltGenerationService xsltGenerationService;

    /**
     * GET /v1/projects/{id}/generate/xslt
     *
     * <p>Returns a MarkLogic-compatible XSLT 2.0 stylesheet derived from the project's
     * XML document mapping.  No database connection is required — the stylesheet is built
     * from mapping metadata alone.  Safe to call repeatedly — read-only.</p>
     *
     * <p>The generated stylesheet is a working identity-style transform with one named
     * template per table mapping.  Developers can deploy it to MarkLogic and use it as a
     * server-side transform via the {@code --transform} CLI option or the {@code transformName}
     * migration request field.</p>
     */
    @GetMapping(value = "/v1/projects/{id}/generate/xslt", produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<String> xslt(@PathVariable String id) {
        try {
            String xslt = xsltGenerationService.generateXslt(id);
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_XML)
                    .body(xslt);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body("XSLT generation failed: " + e.getMessage());
        }
    }
}
