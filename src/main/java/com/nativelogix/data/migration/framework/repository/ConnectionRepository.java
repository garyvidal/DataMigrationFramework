package com.nativelogix.data.migration.framework.repository;

import com.nativelogix.data.migration.framework.model.SavedConnection;

import java.util.List;
import java.util.Optional;

public interface ConnectionRepository {
    SavedConnection save(SavedConnection savedConnection);
    /** Update an existing connection by originalName; keeps existing password if the new one is blank. */
    SavedConnection update(String originalName, SavedConnection updated);
    Optional<SavedConnection> findByName(String name);
    List<SavedConnection> findAll();
    void delete(String name);
    boolean exists(String name);
}
