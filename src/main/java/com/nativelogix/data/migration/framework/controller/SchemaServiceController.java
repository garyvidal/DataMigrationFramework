package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.model.requests.SchemaAnalysisRequest;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import com.nativelogix.data.migration.framework.service.SchemaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
public class SchemaServiceController {

    private final SchemaService schemaService;
    private final PasswordEncryptionService encryptionService;

    @Autowired
    public SchemaServiceController(SchemaService schemaService, PasswordEncryptionService encryptionService) {
        this.schemaService = schemaService;
        this.encryptionService = encryptionService;
    }

    @GetMapping("/api-status")
    public ResponseEntity<String> hello() {
        return ResponseEntity.ok("API is running");
    }

    @GetMapping("/v1/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "API is running");
        response.put("timestamp", new java.util.Date().toString());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/v1/encrypt")
    public ResponseEntity<Map<String, String>> encrypt(@RequestBody Map<String, String> body) {
        String encrypted = encryptionService.encrypt(body.get("value"));
        return ResponseEntity.ok(Map.of("encrypted", encrypted));
    }

    @PostMapping("/v1/schemas")
    public ResponseEntity<?> post(@RequestBody SchemaAnalysisRequest request) {
        try {
            return ResponseEntity.ok(schemaService.analyzeSchema(request));
        } catch (Exception ex) {
            log.error("Schema analysis failed", ex);
            return ResponseEntity.internalServerError().body(Map.of("error", ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName()));
        }
    }
}
