package com.nativelogix.rdbms2marklogic.controller;

import com.nativelogix.rdbms2marklogic.model.migration.DeploymentJob;
import com.nativelogix.rdbms2marklogic.model.migration.MigrationProgress;
import com.nativelogix.rdbms2marklogic.model.migration.MigrationRequest;
import com.nativelogix.rdbms2marklogic.service.migration.MigrationJobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@CrossOrigin(origins = "http://localhost:5173", allowedHeaders = "*",
        methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.OPTIONS})
public class MigrationController {

    private final MigrationJobService migrationJobService;

    /** Start a new migration job */
    @PostMapping("/v1/migration/jobs")
    public ResponseEntity<DeploymentJob> startJob(@RequestBody MigrationRequest request) {
        if (request.getProjectId() == null || request.getProjectId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        if (request.getMarklogicConnectionId() == null || request.getMarklogicConnectionId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        DeploymentJob job = migrationJobService.startJob(request);
        return ResponseEntity.ok(job);
    }

    /** List all migration jobs */
    @GetMapping("/v1/migration/jobs")
    public ResponseEntity<List<DeploymentJob>> getAllJobs() {
        return ResponseEntity.ok(migrationJobService.getAllJobs());
    }

    /** Get progress for a specific job */
    @GetMapping("/v1/migration/jobs/{id}/progress")
    public ResponseEntity<MigrationProgress> getProgress(@PathVariable String id) {
        return migrationJobService.getProgress(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** Delete a completed or failed job record */
    @DeleteMapping("/v1/migration/jobs/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable String id) {
        migrationJobService.deleteJob(id);
        return ResponseEntity.noContent().build();
    }
}
