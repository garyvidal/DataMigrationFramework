package com.nativelogix.data.migration.framework.service;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicPermission;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.marklogic.RoleValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles MarkLogic document-level security: building {@link DocumentMetadataHandle}
 * instances from {@link MarkLogicSecurityConfig}, merging project and job configs,
 * and validating roles against the MarkLogic Management API (best-effort).
 */
@Slf4j
@Service
public class MarkLogicSecurityService {

    /** Default MarkLogic Management API port. */
    private static final int MANAGEMENT_PORT = 8002;

    private final HttpClient httpClient;

    public MarkLogicSecurityService() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    // ── Metadata handle ───────────────────────────────────────────────────────

    /**
     * Builds a {@link DocumentMetadataHandle} from the supplied security config.
     * Returns {@code null} if the config is null or has no effective content,
     * which signals the writer to omit metadata (MarkLogic default permissions apply).
     */
    public DocumentMetadataHandle buildMetadataHandle(MarkLogicSecurityConfig config) {
        if (config == null) return null;

        boolean hasCollections  = hasContent(config.getCollections());
        boolean hasPermissions  = hasContent(config.getPermissions());
        boolean hasQuality      = config.getQuality() != null;
        boolean hasMetadata     = hasContent(config.getMetadata());

        if (!hasCollections && !hasPermissions && !hasQuality && !hasMetadata) return null;

        DocumentMetadataHandle handle = new DocumentMetadataHandle();

        if (hasCollections) {
            handle.getCollections().addAll(config.getCollections());
        }

        if (hasPermissions) {
            for (MarkLogicPermission perm : config.getPermissions()) {
                if (perm.getRoleName() == null || perm.getRoleName().isBlank()) continue;
                if (!hasContent(perm.getCapabilities())) continue;
                DocumentMetadataHandle.Capability[] caps = perm.getCapabilities().stream()
                        .map(c -> {
                            // MarkLogic uses "node-update" as enum name NODE_UPDATE
                            String normalized = c.toUpperCase().replace('-', '_');
                            return DocumentMetadataHandle.Capability.valueOf(normalized);
                        })
                        .toArray(DocumentMetadataHandle.Capability[]::new);
                handle.getPermissions().add(perm.getRoleName(), caps);
            }
        }

        if (hasQuality) {
            handle.setQuality(config.getQuality());
        }

        if (hasMetadata) {
            config.getMetadata().forEach((k, v) -> handle.getMetadataValues().put(k, v));
        }

        return handle;
    }

    // ── Config merging ────────────────────────────────────────────────────────

    /**
     * Merges project-level and job-level security configs.
     * <ul>
     *   <li>permissions — job overrides project entirely if set; otherwise project permissions apply</li>
     *   <li>collections — union of both (deduplicated)</li>
     *   <li>quality     — job overrides project if set</li>
     *   <li>metadata    — merged map; job keys take precedence over project keys</li>
     * </ul>
     * Returns {@code null} only when both inputs are null.
     */
    public MarkLogicSecurityConfig mergeConfigs(MarkLogicSecurityConfig projectConfig,
                                                MarkLogicSecurityConfig jobConfig) {
        if (projectConfig == null && jobConfig == null) return null;
        if (projectConfig == null) return jobConfig;
        if (jobConfig == null) return projectConfig;

        MarkLogicSecurityConfig merged = new MarkLogicSecurityConfig();

        // Permissions: job overrides project
        merged.setPermissions(
                hasContent(jobConfig.getPermissions())
                        ? jobConfig.getPermissions()
                        : projectConfig.getPermissions()
        );

        // Collections: union
        List<String> mergedCollections = new ArrayList<>();
        if (hasContent(projectConfig.getCollections())) mergedCollections.addAll(projectConfig.getCollections());
        if (hasContent(jobConfig.getCollections())) {
            jobConfig.getCollections().stream()
                    .filter(c -> !mergedCollections.contains(c))
                    .forEach(mergedCollections::add);
        }
        merged.setCollections(mergedCollections.isEmpty() ? null : mergedCollections);

        // Quality: job overrides project
        merged.setQuality(jobConfig.getQuality() != null ? jobConfig.getQuality() : projectConfig.getQuality());

        // Metadata: merge, job wins on duplicate keys
        if (hasContent(projectConfig.getMetadata()) || hasContent(jobConfig.getMetadata())) {
            Map<String, String> mergedMeta = new LinkedHashMap<>();
            if (hasContent(projectConfig.getMetadata())) mergedMeta.putAll(projectConfig.getMetadata());
            if (hasContent(jobConfig.getMetadata()))     mergedMeta.putAll(jobConfig.getMetadata());
            merged.setMetadata(mergedMeta);
        }

        return merged;
    }

    // ── Role validation ───────────────────────────────────────────────────────

    /**
     * Best-effort role validation against the MarkLogic Management API (port {@value #MANAGEMENT_PORT}).
     * <p>
     * If the management port is unreachable, the caller lacks {@code manage-user} privilege, or any
     * network error occurs, affected roles are added to {@link RoleValidationResult#unvalidatedRoles()}
     * rather than being reported as missing — the caller should treat these as warnings, not errors.
     */
    public RoleValidationResult validateRoles(MarkLogicSecurityConfig config,
                                              MarkLogicConnection conn,
                                              String plainPassword) {
        if (config == null || !hasContent(config.getPermissions())) {
            return RoleValidationResult.ok();
        }

        List<String> roleNames = config.getPermissions().stream()
                .map(MarkLogicPermission::getRoleName)
                .filter(r -> r != null && !r.isBlank())
                .distinct()
                .toList();

        List<String> missing      = new ArrayList<>();
        List<String> unvalidated  = new ArrayList<>();
        String authHeader         = buildBasicAuthHeader(conn.getUsername(), plainPassword);

        for (String roleName : roleNames) {
            try {
                String encoded = URLEncoder.encode(roleName, StandardCharsets.UTF_8);
                URI uri = URI.create("http://" + conn.getHost() + ":" + MANAGEMENT_PORT
                        + "/manage/v2/roles/" + encoded + "?format=json");

                HttpRequest request = HttpRequest.newBuilder(uri)
                        .header("Authorization", authHeader)
                        .timeout(Duration.ofSeconds(3))
                        .GET()
                        .build();

                HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

                if (response.statusCode() == 404) {
                    log.warn("MarkLogic role not found: '{}'", roleName);
                    missing.add(roleName);
                } else if (response.statusCode() != 200) {
                    log.debug("Role validation inconclusive for '{}': HTTP {}", roleName, response.statusCode());
                    unvalidated.add(roleName);
                }
            } catch (Exception e) {
                log.debug("Role validation skipped for '{}': {}", roleName, e.getMessage());
                unvalidated.add(roleName);
            }
        }

        return new RoleValidationResult(missing.isEmpty(), missing, unvalidated);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private String buildBasicAuthHeader(String username, String password) {
        byte[] credentials = (username + ":" + password).getBytes(StandardCharsets.UTF_8);
        return "Basic " + Base64.getEncoder().encodeToString(credentials);
    }

    private boolean hasContent(List<?> list) {
        return list != null && !list.isEmpty();
    }

    private boolean hasContent(Map<?, ?> map) {
        return map != null && !map.isEmpty();
    }
}
