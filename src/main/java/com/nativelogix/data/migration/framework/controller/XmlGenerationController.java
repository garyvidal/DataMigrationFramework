package com.nativelogix.rdbms2marklogic.controller;

import com.nativelogix.rdbms2marklogic.model.generate.XmlGenerationRequest;
import com.nativelogix.rdbms2marklogic.model.generate.XmlPreviewResult;
import com.nativelogix.rdbms2marklogic.service.generate.XmlGenerationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class XmlGenerationController {

    private final XmlGenerationService xmlGenerationService;

    /**
     * POST /v1/projects/{id}/generate/preview
     *
     * <p>Returns up to {@code limit} XML documents generated from the project's mapping
     * and the live RDBMS data. Safe to call repeatedly — read-only.</p>
     */
    @PostMapping("/v1/projects/{id}/generate/preview")
    public ResponseEntity<XmlPreviewResult> preview(
            @PathVariable String id,
            @RequestBody(required = false) XmlGenerationRequest request) {

        if (request == null) request = new XmlGenerationRequest();
        int limit = Math.max(1, Math.min(request.getLimit(), 100));  // clamp 1–100

        try {
            XmlPreviewResult result = xmlGenerationService.generatePreview(id, limit);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            XmlPreviewResult error = new XmlPreviewResult();
            error.getErrors().add(e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }
}
