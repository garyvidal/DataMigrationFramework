package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.migration.DeploymentJob;
import com.nativelogix.data.migration.framework.model.migration.MigrationPreviewResult;
import com.nativelogix.data.migration.framework.model.migration.MigrationProgress;
import com.nativelogix.data.migration.framework.model.migration.MigrationRequest;
import com.nativelogix.data.migration.framework.model.validation.ValidationReport;
import com.nativelogix.data.migration.framework.service.migration.MigrationJobService;
import com.nativelogix.data.migration.framework.service.migration.MigrationValidationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class MigrationController {

    private final MigrationJobService migrationJobService;
    private final MigrationValidationService migrationValidationService;

    /** Run pre-flight validation checks without starting a job */
    @PostMapping("/v1/migration/validate")
    public ResponseEntity<ValidationReport> validate(@RequestBody MigrationRequest request) {
        if (request.getProjectId() == null || request.getProjectId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        if (request.getMarklogicConnectionId() == null || request.getMarklogicConnectionId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(migrationValidationService.validate(request));
    }

    /** Preview row counts per table before running a migration */
    @GetMapping("/v1/migration/preview/{projectId}")
    public ResponseEntity<MigrationPreviewResult> preview(
            @PathVariable String projectId,
            @RequestParam(required = false) String connectionId) {
        if (projectId == null || projectId.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(migrationJobService.getPreview(projectId, connectionId));
    }

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

    /** SSE stream: pushes MigrationProgress events at ~10% milestones; sends "complete" and closes on job end */
    @GetMapping(value = "/v1/migration/jobs/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamProgress(@PathVariable String id) {
        return migrationJobService.createSseEmitter(id);
    }

    /** Get the security config for a specific job */
    @GetMapping("/v1/migration/jobs/{id}/security")
    public ResponseEntity<MarkLogicSecurityConfig> getJobSecurity(@PathVariable String id) {
        return migrationJobService.getJobSecurity(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> migrationJobService.getJob(id).isPresent()
                        ? ResponseEntity.ok(null)
                        : ResponseEntity.notFound().build());
    }

    /** Update the security config for a specific job */
    @PutMapping("/v1/migration/jobs/{id}/security")
    public ResponseEntity<MarkLogicSecurityConfig> updateJobSecurity(
            @PathVariable String id,
            @RequestBody MarkLogicSecurityConfig securityConfig) {
        return migrationJobService.updateJobSecurity(id, securityConfig)
                .map(job -> ResponseEntity.ok(job.getSecurityConfig()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
