package com.nativelogix.data.migration.framework.repository;

import com.nativelogix.data.migration.framework.model.project.Project;

import java.util.List;
import java.util.Optional;

public interface ProjectRepository {
    Project save(String id, Project project);
    Optional<Project> findById(String id);
    List<Project> findAll();
    void delete(String id);
    boolean exists(String id);
}
