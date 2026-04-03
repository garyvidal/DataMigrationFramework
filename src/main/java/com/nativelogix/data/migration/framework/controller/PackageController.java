package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.model.ImportResult;
import com.nativelogix.data.migration.framework.model.MigrationPackage;
import com.nativelogix.data.migration.framework.service.PackageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/packages")
@RequiredArgsConstructor
public class PackageController {

    private final PackageService packageService;

    /**
     * Export a project (plus optional connections) as a portable package JSON file.
     *
     * GET /v1/packages/export/{projectId}
     *   ?sourceConnectionId=...       (name or UUID, optional)
     *   &marklogicConnectionId=...    (name or UUID, optional)
     *
     * Returns the package as a downloadable {@code .json} attachment.
     */
    @GetMapping("/export/{projectId}")
    public ResponseEntity<MigrationPackage> exportPackage(
            @PathVariable String projectId,
            @RequestParam(required = false) String sourceConnectionId,
            @RequestParam(required = false) String marklogicConnectionId) {

        MigrationPackage pkg = packageService.exportPackage(projectId, sourceConnectionId, marklogicConnectionId);

        String projectName = pkg.getProject() != null && pkg.getProject().getName() != null
                ? pkg.getProject().getName().replaceAll("[^a-zA-Z0-9_\\-]", "_")
                : projectId;
        String filename = projectName + "-package.json";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentDisposition(
                ContentDisposition.attachment().filename(filename).build());

        return ResponseEntity.ok()
                .headers(headers)
                .contentType(MediaType.APPLICATION_JSON)
                .body(pkg);
    }

    /**
     * Import a package: creates the project and connections if they don't already exist.
     * Existing entities (matched by ID) are left untouched.
     *
     * POST /v1/packages/import
     *   ?sourcePassword=...       (plaintext, optional — sets the source connection password)
     *   &marklogicPassword=...    (plaintext, optional — sets the MarkLogic connection password)
     *
     * Body: {@link MigrationPackage} JSON
     */
    @PostMapping("/import")
    public ResponseEntity<ImportResult> importPackage(
            @RequestBody MigrationPackage pkg,
            @RequestParam(required = false) String sourcePassword,
            @RequestParam(required = false) String marklogicPassword) {

        ImportResult result = packageService.importPackage(pkg, sourcePassword, marklogicPassword);
        return ResponseEntity.ok(result);
    }
}
