package com.nativelogix.data.migration.framework.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class FileSystemConnectionRepository implements ConnectionRepository {

    private final Path connectionsDir;
    private final ObjectMapper objectMapper;
    private final PasswordEncryptionService encryptionService;

    public FileSystemConnectionRepository(PasswordEncryptionService encryptionService) {
        this.encryptionService = encryptionService;
        this.objectMapper = new ObjectMapper();
        String userHome = System.getProperty("user.home");
        this.connectionsDir = Paths.get(userHome, ".datamigrationframework", "connections");

        try {
            Files.createDirectories(connectionsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create connections directory: " + e.getMessage(), e);
        }
    }

    /**
     * Saves a connection using {id}.json as the filename.
     * The password is encrypted at rest; the returned object retains plaintext for in-memory use.
     */
    @Override
    public SavedConnection save(SavedConnection savedConnection) {
        try {
            if (savedConnection.getId() == null || savedConnection.getId().isBlank()) {
                savedConnection.setId(UUID.randomUUID().toString());
            }
            SavedConnection toWrite = withEncryptedPassword(savedConnection);
            Path filePath = connectionsDir.resolve(savedConnection.getId() + ".json");
            objectMapper.writeValue(filePath.toFile(), toWrite);
            return savedConnection; // return original (plaintext password in memory)
        } catch (IOException e) {
            throw new RuntimeException("Failed to save connection: " + e.getMessage(), e);
        }
    }

    /**
     * Updates an existing connection. If the new password is blank, the stored password is preserved.
     * Since files are keyed by id, renames no longer require a file delete/recreate.
     */
    @Override
    public SavedConnection update(String originalName, SavedConnection updated) {
        String newPassword = updated.getConnection().getPassword();
        if (newPassword == null || newPassword.isBlank()) {
            // Keep existing password
            findByName(originalName).ifPresent(existing ->
                    updated.getConnection().setPassword(existing.getConnection().getPassword()));
        }
        return save(updated);
    }

    @Override
    public Optional<SavedConnection> findByName(String name) {
        return findAll().stream()
                .filter(sc -> name.equals(sc.getName()))
                .findFirst();
    }

    @Override
    public List<SavedConnection> findAll() {
        try {
            if (!Files.exists(connectionsDir)) {
                return new ArrayList<>();
            }
            // Collect paths first to avoid ConcurrentModificationException during migration
            List<Path> paths = Files.list(connectionsDir)
                    .filter(path -> path.toString().endsWith(".json"))
                    .collect(Collectors.toList());
            List<SavedConnection> results = new ArrayList<>();
            for (Path path : paths) {
                String filenameStem = path.getFileName().toString();
                filenameStem = filenameStem.substring(0, filenameStem.length() - 5);
                try {
                    results.add(readAndMigrate(filenameStem, path.toFile()));
                } catch (Exception e) {
                    log.warn("Skipping unreadable connection file {}: {}", path.getFileName(), e.getMessage());
                }
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to list connections: " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String name) {
        findAll().stream()
                .filter(sc -> name.equals(sc.getName()))
                .findFirst()
                .ifPresent(sc -> {
                    try {
                        Files.deleteIfExists(connectionsDir.resolve(sc.getId() + ".json"));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete connection: " + e.getMessage(), e);
                    }
                });
    }

    @Override
    public boolean exists(String name) {
        return findAll().stream().anyMatch(sc -> name.equals(sc.getName()));
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Reads a connection file and returns a SavedConnection with the password decrypted in memory.
     * Migrates legacy formats transparently:
     *   - bare Connection object → wrapped in SavedConnection
     *   - missing id → assigned and persisted
     *   - plaintext password → re-saved with encryption
     *   - name-based filename ({name}.json) → re-saved as {id}.json, old file deleted
     */
    private SavedConnection readAndMigrate(String filenameStem, File file) throws IOException {
        JsonNode root = objectMapper.readTree(file);
        SavedConnection sc;

        if (root.has("connection")) {
            sc = objectMapper.treeToValue(root, SavedConnection.class);
        } else {
            // Old format — bare Connection object; wrap it
            Connection connection = objectMapper.treeToValue(root, Connection.class);
            sc = new SavedConnection(UUID.randomUUID().toString(), filenameStem, null, connection);
        }

        if (sc.getId() == null || sc.getId().isBlank()) {
            sc.setId(UUID.randomUUID().toString());
        }

        // Decrypt password for in-memory use; detect legacy plaintext for migration
        String storedPassword = sc.getConnection() != null ? sc.getConnection().getPassword() : null;
        boolean isLegacyPlaintext = storedPassword != null && !storedPassword.isEmpty()
                && !storedPassword.startsWith(PasswordEncryptionService.ENC_PREFIX);

        if (sc.getConnection() != null) {
            try {
                sc.getConnection().setPassword(encryptionService.decrypt(storedPassword));
            } catch (Exception e) {
                log.warn("Could not decrypt password for connection '{}' (key mismatch?). Password cleared — re-enter and save.", sc.getName());
                sc.getConnection().setPassword(null);
            }
        }

        // Detect legacy name-based filename: stem is the connection name, not its id
        boolean isLegacyFilename = !filenameStem.equals(sc.getId());

        // Persist migration: re-save as {id}.json if anything needs updating
        boolean formatMigrated = !root.has("connection") || (sc.getId() != null && !root.has("id"));
        if (isLegacyPlaintext || formatMigrated || isLegacyFilename) {
            save(sc); // writes {id}.json
            if (isLegacyFilename) {
                Files.deleteIfExists(file.toPath()); // remove old {name}.json
            }
        }

        return sc;
    }

    /**
     * Returns a shallow copy of the SavedConnection with the password encrypted,
     * suitable for writing to disk. The original object is not modified.
     */
    private SavedConnection withEncryptedPassword(SavedConnection sc) {
        if (sc.getConnection() == null) return sc;
        Connection original = sc.getConnection();
        Connection encrypted = objectMapper.convertValue(original, Connection.class);
        encrypted.setPassword(encryptionService.encrypt(original.getPassword()));
        return new SavedConnection(sc.getId(), sc.getName(), sc.getEnvironment(), encrypted);
    }
}
