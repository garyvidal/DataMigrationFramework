package com.nativelogix.rdbms2marklogic.repository;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nativelogix.rdbms2marklogic.model.project.Project;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
public class FileSystemProjectRepository implements ProjectRepository {

    private final Path projectsDir;
    private final ObjectMapper objectMapper;

    public FileSystemProjectRepository() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String userHome = System.getProperty("user.home");
        this.projectsDir = Paths.get(userHome, ".rdbms2marklogic", "projects");

        try {
            Files.createDirectories(projectsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create projects directory: " + e.getMessage(), e);
        }
    }

    @Override
    public Project save(String id, Project project) {
        try {
            Path filePath = projectsDir.resolve(id + ".json");
            objectMapper.writeValue(filePath.toFile(), project);
            return project;
        } catch (IOException e) {
            throw new RuntimeException("Failed to save project: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<Project> findById(String id) {
        try {
            Path filePath = projectsDir.resolve(id + ".json");
            if (Files.exists(filePath)) {
                Project project = objectMapper.readValue(filePath.toFile(), Project.class);
                return Optional.of(project);
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read project: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Project> findAll() {
        try {
            if (!Files.exists(projectsDir)) {
                return new ArrayList<>();
            }
            return Files.list(projectsDir)
                    .filter(path -> path.toString().endsWith(".json"))
                    .map(path -> {
                        try {
                            return objectMapper.readValue(path.toFile(), Project.class);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to read project file: " + path, e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list projects: " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String id) {
        try {
            Path filePath = projectsDir.resolve(id + ".json");
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete project: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean exists(String id) {
        Path filePath = projectsDir.resolve(id + ".json");
        return Files.exists(filePath);
    }
}
