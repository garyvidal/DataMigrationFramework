package com.nativelogix.data.migration.framework.repository;

import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;

import java.util.List;
import java.util.Optional;

public interface MarkLogicConnectionRepository {
    SavedMarkLogicConnection save(SavedMarkLogicConnection connection);
    /** Update by originalName; keeps existing password if the new one is blank. */
    SavedMarkLogicConnection update(String originalName, SavedMarkLogicConnection updated);
    Optional<SavedMarkLogicConnection> findByName(String name);
    List<SavedMarkLogicConnection> findAll();
    void delete(String name);
    boolean exists(String name);
}
