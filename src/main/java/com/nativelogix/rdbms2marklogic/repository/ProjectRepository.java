package com.nativelogix.rdbms2marklogic.repository;

import com.nativelogix.rdbms2marklogic.model.project.Project;

import java.util.List;
import java.util.Optional;

public interface ProjectRepository {
    Project save(String id, Project project);
    Optional<Project> findById(String id);
    List<Project> findAll();
    void delete(String id);
    boolean exists(String id);
}
