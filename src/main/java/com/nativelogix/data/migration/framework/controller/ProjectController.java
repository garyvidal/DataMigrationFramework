package com.nativelogix.rdbms2marklogic.controller;

import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.repository.FileSystemProjectRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.List;

@RestController
public class ProjectController {

    @Autowired
    private FileSystemProjectRepository projectRepository;

    @PostMapping("/v1/projects")
    public ResponseEntity<Project> saveProject(@RequestBody Project project) {
        if (project.getName() == null || project.getName().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        // Use id as the file key; fall back to name for legacy projects without an id
        if (project.getId() == null || project.getId().isBlank()) {
            project.setId(project.getName());
        }
        OffsetDateTime now = OffsetDateTime.now();
        if (project.getCreated() == null) {
            project.setCreated(now);
        }
        project.setModified(now);
        Project saved = projectRepository.save(project.getId(), project);
        return ResponseEntity.ok(saved);
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
}
