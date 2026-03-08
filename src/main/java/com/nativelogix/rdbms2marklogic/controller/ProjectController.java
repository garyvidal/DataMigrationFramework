package com.nativelogix.rdbms2marklogic.controller;

import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.repository.FileSystemProjectRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.List;

@RestController
@CrossOrigin(origins = "http://localhost:5173", allowedHeaders = "*",
        methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.OPTIONS})
public class ProjectController {

    @Autowired
    private FileSystemProjectRepository projectRepository;

    @PostMapping("/v1/projects")
    public ResponseEntity<Project> saveProject(@RequestBody Project project) {
        if (project.getName() == null || project.getName().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        OffsetDateTime now = OffsetDateTime.now();
        if (project.getCreated() == null) {
            project.setCreated(now);
        }
        project.setModified(now);
        Project saved = projectRepository.save(project.getName(), project);
        return ResponseEntity.ok(saved);
    }

    @GetMapping("/v1/projects")
    public ResponseEntity<List<Project>> getAllProjects() {
        return ResponseEntity.ok(projectRepository.findAll());
    }

    @GetMapping("/v1/projects/{name}")
    public ResponseEntity<Project> getProject(@PathVariable String name) {
        return projectRepository.findByName(name)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/v1/projects/{name}")
    public ResponseEntity<Void> deleteProject(@PathVariable String name) {
        if (!projectRepository.exists(name)) {
            return ResponseEntity.notFound().build();
        }
        projectRepository.delete(name);
        return ResponseEntity.noContent().build();
    }
}
