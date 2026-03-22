package com.nativelogix.rdbms2marklogic.repository;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nativelogix.rdbms2marklogic.model.migration.DeploymentJob;
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
public class FileSystemDeploymentJobRepository implements DeploymentJobRepository {

    private final Path jobsDir;
    private final ObjectMapper objectMapper;

    public FileSystemDeploymentJobRepository() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String userHome = System.getProperty("user.home");
        this.jobsDir = Paths.get(userHome, ".rdbms2marklogic", "migration-jobs");
        try {
            Files.createDirectories(jobsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create migration jobs directory: " + e.getMessage(), e);
        }
    }

    @Override
    public DeploymentJob save(DeploymentJob job) {
        try {
            Path filePath = jobsDir.resolve(job.getId() + ".json");
            objectMapper.writeValue(filePath.toFile(), job);
            return job;
        } catch (IOException e) {
            throw new RuntimeException("Failed to save deployment job: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<DeploymentJob> findById(String id) {
        try {
            Path filePath = jobsDir.resolve(id + ".json");
            if (Files.exists(filePath)) {
                return Optional.of(objectMapper.readValue(filePath.toFile(), DeploymentJob.class));
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read deployment job: " + e.getMessage(), e);
        }
    }

    @Override
    public List<DeploymentJob> findAll() {
        try {
            if (!Files.exists(jobsDir)) return new ArrayList<>();
            return Files.list(jobsDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try {
                            return objectMapper.readValue(p.toFile(), DeploymentJob.class);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to read job file: " + p, e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list deployment jobs: " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String id) {
        try {
            Files.deleteIfExists(jobsDir.resolve(id + ".json"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete deployment job: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean exists(String id) {
        return Files.exists(jobsDir.resolve(id + ".json"));
    }
}
