package com.nativelogix.data.migration.framework.repository;

import com.nativelogix.data.migration.framework.model.migration.DeploymentJob;

import java.util.List;
import java.util.Optional;

public interface DeploymentJobRepository {
    DeploymentJob save(DeploymentJob job);
    Optional<DeploymentJob> findById(String id);
    List<DeploymentJob> findAll();
    void delete(String id);
    boolean exists(String id);
}
