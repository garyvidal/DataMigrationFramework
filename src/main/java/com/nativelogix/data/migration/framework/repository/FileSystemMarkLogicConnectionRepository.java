package com.nativelogix.data.migration.framework.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Persists MarkLogic connections as JSON files under
 * {@code ~/.datamigrationframework/marklogic-connections/{id}.json}.
 * Passwords are encrypted at rest using {@link PasswordEncryptionService}.
 */
@Slf4j
@Repository
public class FileSystemMarkLogicConnectionRepository implements MarkLogicConnectionRepository {

    private final Path connectionsDir;
    private final ObjectMapper objectMapper;
    private final PasswordEncryptionService encryptionService;

    public FileSystemMarkLogicConnectionRepository(PasswordEncryptionService encryptionService) {
        this.encryptionService = encryptionService;
        this.objectMapper = new ObjectMapper();
        this.connectionsDir = Paths.get(
                System.getProperty("user.home"), ".datamigrationframework", "marklogic-connections");
        try {
            Files.createDirectories(connectionsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create MarkLogic connections directory: " + e.getMessage(), e);
        }
    }

    @Override
    public SavedMarkLogicConnection save(SavedMarkLogicConnection sc) {
        try {
            if (sc.getId() == null || sc.getId().isBlank()) {
                sc.setId(UUID.randomUUID().toString());
            }
            SavedMarkLogicConnection toWrite = withEncryptedPassword(sc);
            Path filePath = connectionsDir.resolve(sc.getId() + ".json");
            objectMapper.writeValue(filePath.toFile(), toWrite);
            return sc; // return original (plaintext password in memory)
        } catch (IOException e) {
            throw new RuntimeException("Failed to save MarkLogic connection: " + e.getMessage(), e);
        }
    }

    @Override
    public SavedMarkLogicConnection update(String originalName, SavedMarkLogicConnection updated) {
        String newPassword = updated.getConnection() != null ? updated.getConnection().getPassword() : null;
        if (newPassword == null || newPassword.isBlank()) {
            // Keep existing stored password
            findByName(originalName).ifPresent(existing -> {
                if (existing.getConnection() != null && updated.getConnection() != null) {
                    updated.getConnection().setPassword(existing.getConnection().getPassword());
                }
            });
        }
        return save(updated);
    }

    @Override
    public Optional<SavedMarkLogicConnection> findByName(String name) {
        return findAll().stream()
                .filter(sc -> name.equals(sc.getName()))
                .findFirst();
    }

    @Override
    public List<SavedMarkLogicConnection> findAll() {
        try {
            if (!Files.exists(connectionsDir)) {
                return new ArrayList<>();
            }
            List<Path> paths = Files.list(connectionsDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .collect(Collectors.toList());
            return paths.stream()
                    .map(path -> {
                        try {
                            return readAndDecrypt(path);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to read connection file: " + path, e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list MarkLogic connections: " + e.getMessage(), e);
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
                        throw new RuntimeException("Failed to delete MarkLogic connection: " + e.getMessage(), e);
                    }
                });
    }

    @Override
    public boolean exists(String name) {
        return findAll().stream().anyMatch(sc -> name.equals(sc.getName()));
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Reads a connection file and returns a SavedMarkLogicConnection with the password
     * decrypted in memory. Re-saves if the password was stored as legacy plaintext.
     */
    private SavedMarkLogicConnection readAndDecrypt(Path path) throws IOException {
        SavedMarkLogicConnection sc = objectMapper.readValue(path.toFile(), SavedMarkLogicConnection.class);

        if (sc.getId() == null || sc.getId().isBlank()) {
            sc.setId(UUID.randomUUID().toString());
        }

        if (sc.getConnection() != null) {
            String storedPassword = sc.getConnection().getPassword();
            boolean isLegacyPlaintext = storedPassword != null && !storedPassword.isEmpty()
                    && !storedPassword.startsWith(PasswordEncryptionService.ENC_PREFIX);
            try {
                sc.getConnection().setPassword(encryptionService.decrypt(storedPassword));
            } catch (Exception e) {
                log.warn("Could not decrypt password for connection '{}' (key mismatch?). Password cleared — re-enter and save.", sc.getName());
                sc.getConnection().setPassword(null);
                return sc;
            }
            if (isLegacyPlaintext) {
                save(sc); // re-save with encryption
            }
        }

        return sc;
    }

    /**
     * Returns a copy of the SavedMarkLogicConnection with the password encrypted,
     * suitable for writing to disk. The original object is not modified.
     */
    private SavedMarkLogicConnection withEncryptedPassword(SavedMarkLogicConnection sc) {
        if (sc.getConnection() == null) return sc;
        MarkLogicConnection original = sc.getConnection();
        MarkLogicConnection encrypted = objectMapper.convertValue(original, MarkLogicConnection.class);
        encrypted.setPassword(encryptionService.encrypt(original.getPassword()));
        return new SavedMarkLogicConnection(sc.getId(), sc.getName(), encrypted);
    }
}
