package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.repository.FileSystemProjectRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@RestController
public class ProjectController {

    @Autowired
    private FileSystemProjectRepository projectRepository;

    @PostMapping("/v1/projects")
    public ResponseEntity<Project> saveProject(@RequestBody Project project) {
        if (project.getName() == null || project.getName().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        boolean isNew = (project.getId() == null || project.getId().isBlank());
        if (isNew) {
            // Always generate a UUID for new projects — never use name as id.
            project.setId(UUID.randomUUID().toString());
        }
        // Block duplicate names: reject if another project already has this name.
        projectRepository.findByName(project.getName()).ifPresent(existing -> {
            if (!existing.getId().equals(project.getId())) {
                throw new DuplicateProjectNameException(project.getName());
            }
        });
        OffsetDateTime now = OffsetDateTime.now();
        if (project.getCreated() == null) {
            project.setCreated(now);
        }
        project.setModified(now);
        Project saved = projectRepository.save(project.getId(), project);
        return ResponseEntity.ok(saved);
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(DuplicateProjectNameException.class)
    public ResponseEntity<String> handleDuplicateName(DuplicateProjectNameException ex) {
        return ResponseEntity.status(org.springframework.http.HttpStatus.CONFLICT)
                .body("A project named \"" + ex.getName() + "\" already exists.");
    }

    static class DuplicateProjectNameException extends RuntimeException {
        private final String name;
        DuplicateProjectNameException(String name) {
            super("Duplicate project name: " + name);
            this.name = name;
        }
        String getName() { return name; }
    }

    @GetMapping("/v1/projects")
    public ResponseEntity<List<Project>> getAllProjects() {
        return ResponseEntity.ok(projectRepository.findAll());
    }

    @GetMapping("/v1/projects/{id}")
    public ResponseEntity<Project> getProject(@PathVariable String id) {
        return projectRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/v1/projects/{id}")
    public ResponseEntity<Void> deleteProject(@PathVariable String id) {
        if (!projectRepository.exists(id)) {
            return ResponseEntity.notFound().build();
        }
        projectRepository.delete(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/v1/projects/{id}/security")
    public ResponseEntity<MarkLogicSecurityConfig> getProjectSecurity(@PathVariable String id) {
        return projectRepository.findById(id)
                .map(p -> ResponseEntity.ok(p.getSecurityConfig()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/v1/projects/{id}/security")
    public ResponseEntity<MarkLogicSecurityConfig> updateProjectSecurity(
            @PathVariable String id,
            @RequestBody MarkLogicSecurityConfig securityConfig) {
        return projectRepository.findById(id).map(project -> {
            project.setSecurityConfig(securityConfig);
            project.setModified(OffsetDateTime.now());
            projectRepository.save(id, project);
            return ResponseEntity.ok(securityConfig);
        }).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
