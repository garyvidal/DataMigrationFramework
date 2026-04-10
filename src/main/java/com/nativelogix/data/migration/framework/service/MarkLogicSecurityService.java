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
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        List<String> missing     = new ArrayList<>();
        List<String> unvalidated = new ArrayList<>();
        boolean isBasic          = "basic".equalsIgnoreCase(conn.getAuthType());

        for (String roleName : roleNames) {
            try {
                String encoded = URLEncoder.encode(roleName, StandardCharsets.UTF_8);
                URI uri = URI.create("http://" + conn.getHost() + ":" + MANAGEMENT_PORT
                        + "/manage/v2/roles/" + encoded + "?format=json");
                log.debug("Role Url:{}", uri);

                int status = isBasic
                        ? executeWithBasicAuth(uri, conn.getUsername(), plainPassword)
                        : executeWithDigestAuth(uri, conn.getUsername(), plainPassword);

                if (status == 404) {
                    log.warn("MarkLogic role not found: '{}'", roleName);
                    missing.add(roleName);
                } else if (status != 200) {
                    log.debug("Role validation inconclusive for '{}': HTTP {}", roleName, status);
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

    private int executeWithBasicAuth(URI uri, String username, String password) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Authorization", buildBasicAuthHeader(username, password))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    /**
     * Implements HTTP Digest authentication manually (two-pass):
     * 1. Send unauthenticated request to get the 401 + WWW-Authenticate challenge.
     * 2. Parse realm/nonce/qop, compute the MD5 response, resend with Authorization header.
     *
     * Required because {@code java.net.http.HttpClient}'s Authenticator only handles Basic auth;
     * it does not perform the Digest challenge/response handshake automatically.
     */
    private int executeWithDigestAuth(URI uri, String username, String password) throws Exception {
        // Pass 1: get the challenge
        HttpRequest probe = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
        HttpResponse<Void> challenge = httpClient.send(probe, HttpResponse.BodyHandlers.discarding());
        if (challenge.statusCode() != 401) {
            return challenge.statusCode(); // already authorized or other error
        }

        String wwwAuth = challenge.headers().firstValue("WWW-Authenticate").orElse("");
        if (!wwwAuth.startsWith("Digest ")) {
            log.debug("Unexpected WWW-Authenticate scheme: {}", wwwAuth);
            return 401;
        }

        String realm  = extractDirective(wwwAuth, "realm");
        String nonce  = extractDirective(wwwAuth, "nonce");
        String qop    = extractDirective(wwwAuth, "qop");
        String method = "GET";
        String path   = uri.getRawPath() + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : "");
        String nc     = "00000001";
        String cnonce = UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        String ha1 = md5(username + ":" + realm + ":" + password);
        String ha2 = md5(method + ":" + path);
        String digestResponse = (qop != null && (qop.contains("auth")))
                ? md5(ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":auth:" + ha2)
                : md5(ha1 + ":" + nonce + ":" + ha2);

        StringBuilder authHeader = new StringBuilder("Digest ")
                .append("username=\"").append(username).append("\", ")
                .append("realm=\"").append(realm).append("\", ")
                .append("nonce=\"").append(nonce).append("\", ")
                .append("uri=\"").append(path).append("\", ")
                .append("response=\"").append(digestResponse).append("\"");
        if (qop != null && qop.contains("auth")) {
            authHeader.append(", qop=auth")
                      .append(", nc=").append(nc)
                      .append(", cnonce=\"").append(cnonce).append("\"");
        }

        // Pass 2: send authenticated request
        HttpRequest authenticated = HttpRequest.newBuilder(uri)
                .header("Authorization", authHeader.toString())
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
        return httpClient.send(authenticated, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static String extractDirective(String header, String key) {
        Matcher m = Pattern.compile(key + "=\"?([^,\"]+)\"?").matcher(header);
        return m.find() ? m.group(1).trim() : null;
    }

    private static String md5(String input) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] bytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

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
