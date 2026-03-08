package com.nativelogix.rdbms2marklogic.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nativelogix.rdbms2marklogic.model.Connection;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class FileSystemConnectionRepository implements ConnectionRepository {
    
    private final Path connectionsDir;
    private final ObjectMapper objectMapper;
    
    public FileSystemConnectionRepository() {
        this.objectMapper = new ObjectMapper();
        String userHome = System.getProperty("user.home");
        this.connectionsDir = Paths.get(userHome, ".rdbms2marklogic", "connections");
        
        try {
            Files.createDirectories(connectionsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create connections directory: " + e.getMessage(), e);
        }
    }
    
    @Override
    public Connection save(String name, Connection connection) {
        try {
            Path filePath = connectionsDir.resolve(name + ".json");
            objectMapper.writeValue(filePath.toFile(), connection);
            return connection;
        } catch (IOException e) {
            throw new RuntimeException("Failed to save connection: " + e.getMessage(), e);
        }
    }
    
    @Override
    public Optional<Connection> findByName(String name) {
        try {
            Path filePath = connectionsDir.resolve(name + ".json");
            if (Files.exists(filePath)) {
                Connection connection = objectMapper.readValue(filePath.toFile(), Connection.class);
                return Optional.of(connection);
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read connection: " + e.getMessage(), e);
        }
    }
    
    @Override
    public List<Connection> findAll() {
        try {
            if (!Files.exists(connectionsDir)) {
                return new ArrayList<>();
            }
            return Files.list(connectionsDir)
                    .filter(path -> path.toString().endsWith(".json"))
                    .map(path -> {
                        try {
                            return objectMapper.readValue(path.toFile(), Connection.class);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to read connection file: " + path, e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list connections: " + e.getMessage(), e);
        }
    }

    /**
     * Get all connections with their names (filenames without .json extension)
     */
    public Map<String, Connection> findAllWithNames() {
        try {
            if (!Files.exists(connectionsDir)) {
                return new HashMap<>();
            }
            return Files.list(connectionsDir)
                    .filter(path -> path.toString().endsWith(".json"))
                    .collect(Collectors.toMap(
                            path -> {
                                String filename = path.getFileName().toString();
                                return filename.substring(0, filename.length() - 5); // Remove .json extension
                            },
                            path -> {
                                try {
                                    return objectMapper.readValue(path.toFile(), Connection.class);
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to read connection file: " + path, e);
                                }
                            }
                    ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to list connections: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void delete(String name) {
        try {
            Path filePath = connectionsDir.resolve(name + ".json");
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete connection: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean exists(String name) {
        Path filePath = connectionsDir.resolve(name + ".json");
        return Files.exists(filePath);
    }
}
