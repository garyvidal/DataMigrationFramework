package com.nativelogix.data.migration.framework.service;

import com.nativelogix.data.migration.framework.model.*;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.repository.ConnectionRepository;
import com.nativelogix.data.migration.framework.repository.MarkLogicConnectionRepository;
import com.nativelogix.data.migration.framework.repository.ProjectRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
public class PackageService {

    private final ProjectRepository projectRepository;
    private final ConnectionRepository connectionRepository;
    private final MarkLogicConnectionRepository markLogicConnectionRepository;

    // ── Export ────────────────────────────────────────────────────────────────

    /**
     * Builds a {@link MigrationPackage} for the given project, optionally bundling
     * the source and MarkLogic connections. Passwords are always stripped from the export.
     *
     * @param projectId            required
     * @param sourceConnectionId   name or UUID, may be null
     * @param marklogicConnectionId name or UUID, may be null
     */
    public MigrationPackage exportPackage(String projectId,
                                          String sourceConnectionId,
                                          String marklogicConnectionId) {
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));

        MigrationPackage pkg = new MigrationPackage();
        pkg.setExportedAt(OffsetDateTime.now());
        pkg.setProject(project);

        if (sourceConnectionId != null && !sourceConnectionId.isBlank()) {
            connectionRepository.findAll().stream()
                    .filter(sc -> sourceConnectionId.equals(sc.getId())
                            || sourceConnectionId.equalsIgnoreCase(sc.getName()))
                    .findFirst()
                    .ifPresent(sc -> pkg.setSourceConnection(stripSourcePassword(sc)));
        }

        if (marklogicConnectionId != null && !marklogicConnectionId.isBlank()) {
            markLogicConnectionRepository.findAll().stream()
                    .filter(sc -> marklogicConnectionId.equals(sc.getId())
                            || marklogicConnectionId.equalsIgnoreCase(sc.getName()))
                    .findFirst()
                    .ifPresent(sc -> pkg.setMarklogicConnection(stripMlPassword(sc)));
        }

        return pkg;
    }

    // ── Import ────────────────────────────────────────────────────────────────

    /**
     * Imports a {@link MigrationPackage}: saves the project and connections if they
     * do not already exist (matched by ID). Existing entities are left untouched.
     *
     * @param pkg              the package to import
     * @param sourcePassword   plaintext password for the source connection; may be null
     * @param marklogicPassword plaintext password for the MarkLogic connection; may be null
     * @return {@link ImportResult} describing what was created and any warnings
     */
    public ImportResult importPackage(MigrationPackage pkg,
                                      String sourcePassword,
                                      String marklogicPassword) {
        ImportResult result = new ImportResult();

        // ── Project ───────────────────────────────────────────────────────────
        Project project = pkg.getProject();
        if (project != null && project.getId() != null) {
            result.setProjectId(project.getId());
            result.setProjectName(project.getName());
            if (!projectRepository.exists(project.getId())) {
                projectRepository.save(project.getId(), project);
                result.setProjectCreated(true);
            }
        }

        // ── Source connection ─────────────────────────────────────────────────
        SavedConnection sc = pkg.getSourceConnection();
        if (sc != null && sc.getId() != null) {
            result.setSourceConnectionId(sc.getId());
            result.setSourceConnectionName(sc.getName());
            boolean alreadyExists = connectionRepository.findAll().stream()
                    .anyMatch(existing -> sc.getId().equals(existing.getId()));
            if (!alreadyExists) {
                if (sourcePassword != null && !sourcePassword.isBlank() && sc.getConnection() != null) {
                    sc.getConnection().setPassword(sourcePassword);
                }
                if (sc.getConnection() != null
                        && (sc.getConnection().getPassword() == null || sc.getConnection().getPassword().isBlank())) {
                    result.getWarnings().add(
                            "Source connection '" + sc.getName() + "' was imported without a password. " +
                            "Update it in Connections before running a migration.");
                }
                connectionRepository.save(sc);
                result.setSourceConnectionCreated(true);
            } else if (sourcePassword != null && !sourcePassword.isBlank()) {
                // Connection already registered — update its password with the explicitly provided value
                connectionRepository.findAll().stream()
                        .filter(existing -> sc.getId().equals(existing.getId()))
                        .findFirst()
                        .ifPresent(existing -> {
                            if (existing.getConnection() != null) {
                                existing.getConnection().setPassword(sourcePassword);
                                connectionRepository.save(existing);
                            }
                        });
            }
        }

        // ── MarkLogic connection ──────────────────────────────────────────────
        SavedMarkLogicConnection mlc = pkg.getMarklogicConnection();
        if (mlc != null && mlc.getId() != null) {
            result.setMarklogicConnectionId(mlc.getId());
            result.setMarklogicConnectionName(mlc.getName());
            boolean alreadyExists = markLogicConnectionRepository.findAll().stream()
                    .anyMatch(existing -> mlc.getId().equals(existing.getId()));
            if (!alreadyExists) {
                if (marklogicPassword != null && !marklogicPassword.isBlank() && mlc.getConnection() != null) {
                    mlc.getConnection().setPassword(marklogicPassword);
                }
                if (mlc.getConnection() != null
                        && (mlc.getConnection().getPassword() == null || mlc.getConnection().getPassword().isBlank())) {
                    result.getWarnings().add(
                            "MarkLogic connection '" + mlc.getName() + "' was imported without a password. " +
                            "Update it in MarkLogic Connections before running a migration.");
                }
                markLogicConnectionRepository.save(mlc);
                result.setMarklogicConnectionCreated(true);
            } else if (marklogicPassword != null && !marklogicPassword.isBlank()) {
                // Connection already registered — update its password with the explicitly provided value
                markLogicConnectionRepository.findAll().stream()
                        .filter(existing -> mlc.getId().equals(existing.getId()))
                        .findFirst()
                        .ifPresent(existing -> {
                            if (existing.getConnection() != null) {
                                existing.getConnection().setPassword(marklogicPassword);
                                markLogicConnectionRepository.save(existing);
                            }
                        });
            }
        }

        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private SavedConnection stripSourcePassword(SavedConnection original) {
        Connection conn = null;
        if (original.getConnection() != null) {
            Connection src = original.getConnection();
            conn = new Connection();
            conn.setType(src.getType());
            conn.setUrl(src.getUrl());
            conn.setPort(src.getPort());
            conn.setDatabase(src.getDatabase());
            conn.setUserName(src.getUserName());
            conn.setPassword(null); // always strip
            conn.setEnterUriManually(src.getEnterUriManually());
            conn.setJdbcUri(src.getJdbcUri());
            conn.setAuthentication(src.getAuthentication());
            conn.setIdentifier(src.getIdentifier());
            conn.setPdbName(src.getPdbName());
            conn.setUseSSL(src.getUseSSL());
            conn.setSslMode(src.getSslMode());
        }
        return new SavedConnection(original.getId(), original.getName(), original.getEnvironment(), conn);
    }

    private SavedMarkLogicConnection stripMlPassword(SavedMarkLogicConnection original) {
        MarkLogicConnection conn = null;
        if (original.getConnection() != null) {
            MarkLogicConnection src = original.getConnection();
            conn = new MarkLogicConnection();
            conn.setHost(src.getHost());
            conn.setPort(src.getPort());
            conn.setDatabase(src.getDatabase());
            conn.setUsername(src.getUsername());
            conn.setPassword(null); // always strip
            conn.setAuthType(src.getAuthType());
            conn.setUseSSL(src.getUseSSL());
        }
        return new SavedMarkLogicConnection(original.getId(), original.getName(), conn);
    }
}
