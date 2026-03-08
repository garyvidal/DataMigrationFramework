package com.nativelogix.rdbms2marklogic.repository;

import com.nativelogix.rdbms2marklogic.model.Connection;
import java.util.List;
import java.util.Optional;

public interface ConnectionRepository {
    Connection save(String name, Connection connection);
    Optional<Connection> findByName(String name);
    List<Connection> findAll();
    void delete(String name);
    boolean exists(String name);
}
