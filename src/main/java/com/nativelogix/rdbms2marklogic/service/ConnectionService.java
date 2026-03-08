package com.nativelogix.rdbms2marklogic.service;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.ConnectionTestResult;
import com.nativelogix.rdbms2marklogic.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import us.fatehi.utility.datasource.DatabaseConnectionSourceBuilder;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ConnectionService {
    
    private final ConnectionRepository connectionRepository;
    
    public Connection saveConnection(String name, Connection connection) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Connection name cannot be empty");
        }
        return connectionRepository.save(name, connection);
    }
    
    public Optional<Connection> getConnection(String name) {
        return connectionRepository.findByName(name);
    }
    
    public List<Connection> getAllConnections() {
        return connectionRepository.findAll();
    }
    
    public void deleteConnection(String name) {
        connectionRepository.delete(name);
    }
    
    public boolean connectionExists(String name) {
        return connectionRepository.exists(name);
    }

    public ConnectionTestResult testConnection(Connection connection) {
        try {
            var dataSource = DatabaseConnectionSourceBuilder
                    .builder(buildJdbcUrl(connection))
                    .withUserCredentials(new MultiUseUserCredentials(connection.getUserName(), connection.getPassword()))
                    .build();
            try (java.sql.Connection conn = dataSource.get()) {
                return new ConnectionTestResult(true, "Connection successful");
            }
        } catch (Exception e) {
            return new ConnectionTestResult(false, e.getMessage() != null ? e.getMessage() : "Connection failed");
        }
    }

    private String buildJdbcUrl(Connection connection) {
        String host = connection.getUrl();
        int port = connection.getPort();
        String database = connection.getDatabase();
        return switch (connection.getType()) {
            case MySql -> String.format("jdbc:mysql://%s:%d/%s", host, port, database);
            case SqlServer -> String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, database);
            case Oracle -> String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, database);
            default -> String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        };
    }
}
