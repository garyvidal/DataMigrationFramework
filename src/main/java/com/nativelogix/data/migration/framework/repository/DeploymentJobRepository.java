package com.nativelogix.rdbms2marklogic.repository;

import com.nativelogix.rdbms2marklogic.model.migration.DeploymentJob;

import java.util.List;
import java.util.Optional;

public interface DeploymentJobRepository {
    DeploymentJob save(DeploymentJob job);
    Optional<DeploymentJob> findById(String id);
    List<DeploymentJob> findAll();
    void delete(String id);
    boolean exists(String id);
}
