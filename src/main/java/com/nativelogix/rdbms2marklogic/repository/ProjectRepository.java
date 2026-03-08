package com.nativelogix.rdbms2marklogic.repository;

import com.nativelogix.rdbms2marklogic.model.project.Project;

import java.util.List;
import java.util.Optional;

public interface ProjectRepository {
    Project save(String name, Project project);
    Optional<Project> findByName(String name);
    List<Project> findAll();
    void delete(String name);
    boolean exists(String name);
}
