package com.nativelogix.rdbms2marklogic.controller;

import com.nativelogix.rdbms2marklogic.model.generate.JsonGenerationRequest;
import com.nativelogix.rdbms2marklogic.model.generate.JsonPreviewResult;
import com.nativelogix.rdbms2marklogic.service.generate.JsonGenerationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class JsonGenerationController {

    private final JsonGenerationService jsonGenerationService;

    /**
     * POST /v1/projects/{id}/generate/json/preview
     *
     * <p>Returns up to {@code limit} JSON documents generated from the project's
     * JSON mapping and the live RDBMS data. Safe to call repeatedly — read-only.</p>
     */
    @PostMapping("/v1/projects/{id}/generate/json/preview")
    public ResponseEntity<JsonPreviewResult> preview(
            @PathVariable String id,
            @RequestBody(required = false) JsonGenerationRequest request) {

        if (request == null) request = new JsonGenerationRequest();
        int limit = Math.max(1, Math.min(request.getLimit(), 100));

        try {
            JsonPreviewResult result = jsonGenerationService.generatePreview(id, limit);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            JsonPreviewResult error = new JsonPreviewResult();
            error.getErrors().add(e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }
}
