package com.nativelogix.data.migration.framework.service.migration;

import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.marklogic.RoleValidationResult;
import com.nativelogix.data.migration.framework.model.migration.MigrationRequest;
import com.nativelogix.data.migration.framework.model.project.DocumentModel;
import com.nativelogix.data.migration.framework.model.project.JsonDocumentModel;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.project.ProjectMapping;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import com.nativelogix.data.migration.framework.model.validation.ValidationCheck;
import com.nativelogix.data.migration.framework.model.validation.ValidationReport;
import com.nativelogix.data.migration.framework.repository.FileSystemProjectRepository;
import com.nativelogix.data.migration.framework.model.ConnectionTestResult;
import com.nativelogix.data.migration.framework.service.JDBCConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicSecurityService;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pre-flight validation for migration requests.
 * Runs connectivity, mapping completeness, parentRef integrity, namespace declaration,
 * and security role checks — returning a {@link ValidationReport} the caller can inspect
 * before deciding whether to proceed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MigrationValidationService {

    private static final String CAT_CONNECTIVITY = "CONNECTIVITY";
    private static final String CAT_MAPPING       = "MAPPING";
    private static final String CAT_SECURITY       = "SECURITY";

    private final FileSystemProjectRepository projectRepository;
    private final JDBCConnectionService jdbcConnectionService;
    private final MarkLogicConnectionService markLogicConnectionService;
    private final MarkLogicSecurityService markLogicSecurityService;
    private final PasswordEncryptionService passwordEncryptionService;

    /**
     * Runs all pre-flight checks for the given request and returns a {@link ValidationReport}.
     * Always returns a report — never throws.
     */
    public ValidationReport validate(MigrationRequest request) {
        List<ValidationCheck> checks = new ArrayList<>();

        // ── Resolve project ──────────────────────────────────────────────────────
        Project project;
        try {
            project = projectRepository.findById(request.getProjectId())
                    .orElseThrow(() -> new IllegalArgumentException("Project not found: " + request.getProjectId()));
        } catch (Exception e) {
            checks.add(ValidationCheck.fail("PROJECT_RESOLUTION", CAT_MAPPING,
                    "Project resolution", e.getMessage(),
                    "Ensure the project ID is correct and the project file exists."));
            return new ValidationReport(checks);
        }

        // ── Resolve source connection ────────────────────────────────────────────
        SavedConnection sourceConn = null;
        try {
            String connId = request.getSourceConnectionId();
            if (connId != null && !connId.isBlank()) {
                sourceConn = jdbcConnectionService.getAllConnections().stream()
                        .filter(c -> connId.equals(c.getId()) || connId.equals(c.getName()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Source connection not found: " + connId));
            } else {
                String connName = project.getConnectionName();
                sourceConn = jdbcConnectionService.getConnection(connName)
                        .orElseThrow(() -> new IllegalArgumentException("Connection not found: " + connName));
            }
        } catch (Exception e) {
            checks.add(ValidationCheck.fail("SOURCE_RESOLUTION", CAT_CONNECTIVITY,
                    "Source connection resolution", e.getMessage(),
                    "Select a valid source RDBMS connection."));
        }

        // ── Resolve MarkLogic connection ─────────────────────────────────────────
        SavedMarkLogicConnection mlConn = null;
        try {
            String mlId = request.getMarklogicConnectionId();
            mlConn = markLogicConnectionService.getAllConnections().stream()
                    .filter(c -> mlId.equals(c.getId()) || mlId.equals(c.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("MarkLogic connection not found: " + mlId));
        } catch (Exception e) {
            checks.add(ValidationCheck.fail("MARKLOGIC_RESOLUTION", CAT_CONNECTIVITY,
                    "MarkLogic connection resolution", e.getMessage(),
                    "Select a valid MarkLogic connection."));
        }

        // ── Source DB connectivity ────────────────────────────────────────────────
        if (sourceConn != null) {
            checks.add(checkSourceConnectivity(sourceConn));
        }

        // ── MarkLogic connectivity ────────────────────────────────────────────────
        if (mlConn != null) {
            checks.add(checkMarkLogicConnectivity(mlConn));
        }

        // ── Mapping completeness ──────────────────────────────────────────────────
        checks.add(checkMappingCompleteness(project));

        // ── ParentRef integrity ───────────────────────────────────────────────────
        checks.add(checkParentRefIntegrity(project));

        // ── Namespace declarations ────────────────────────────────────────────────
        checks.add(checkNamespaceDeclarations(project));

        // ── Security role validation ──────────────────────────────────────────────
        if (mlConn != null) {
            MarkLogicSecurityConfig effectiveSecurity = markLogicSecurityService.mergeConfigs(
                    project.getSecurityConfig(), request.getSecurityConfig());
            checks.add(checkSecurityRoles(effectiveSecurity, mlConn));
        }

        return new ValidationReport(checks);
    }

    // ── Connectivity checks ───────────────────────────────────────────────────

    private ValidationCheck checkSourceConnectivity(SavedConnection savedConn) {
        try {
            ConnectionTestResult result = jdbcConnectionService.testConnection(savedConn.getConnection());
            if (result.isSuccess()) {
                return ValidationCheck.pass("SOURCE_CONNECTIVITY", CAT_CONNECTIVITY, "Source DB connection");
            }
            return ValidationCheck.fail("SOURCE_CONNECTIVITY", CAT_CONNECTIVITY,
                    "Source DB connection", result.getMessage(),
                    "Check that the database server is running and credentials are correct.");
        } catch (Exception e) {
            return ValidationCheck.fail("SOURCE_CONNECTIVITY", CAT_CONNECTIVITY,
                    "Source DB connection", e.getMessage(),
                    "Check that the database server is running and credentials are correct.");
        }
    }

    private ValidationCheck checkMarkLogicConnectivity(SavedMarkLogicConnection mlConn) {
        try {
            ConnectionTestResult result = markLogicConnectionService.testConnection(mlConn.getConnection());
            if (result.isSuccess()) {
                return ValidationCheck.pass("MARKLOGIC_CONNECTIVITY", CAT_CONNECTIVITY, "MarkLogic connection");
            }
            return ValidationCheck.fail("MARKLOGIC_CONNECTIVITY", CAT_CONNECTIVITY,
                    "MarkLogic connection", result.getMessage(),
                    "Check that the MarkLogic app server is reachable and credentials are correct.");
        } catch (Exception e) {
            return ValidationCheck.fail("MARKLOGIC_CONNECTIVITY", CAT_CONNECTIVITY,
                    "MarkLogic connection", e.getMessage(),
                    "Check that the MarkLogic app server is reachable and credentials are correct.");
        }
    }

    // ── Mapping checks ────────────────────────────────────────────────────────

    private ValidationCheck checkMappingCompleteness(Project project) {
        ProjectMapping mapping = project.getMapping();
        if (mapping == null) {
            return ValidationCheck.fail("MAPPING_COMPLETENESS", CAT_MAPPING,
                    "Mapping completeness", "No document mapping configured.",
                    "Open the project and define a document mapping before migrating.");
        }

        String type = mapping.getMappingType() != null ? mapping.getMappingType().toUpperCase() : "XML";
        List<String> failures = new ArrayList<>();

        if (!type.equals("JSON")) {
            // Must have XML mapping
            DocumentModel dm = mapping.getDocumentModel();
            if (dm == null || dm.getRoot() == null) {
                failures.add("XML document model has no root element");
            } else if (isBlank(dm.getRoot().getSourceTable())) {
                failures.add("XML root element has no source table");
            } else if (dm.getRoot().getColumns() == null || dm.getRoot().getColumns().isEmpty()) {
                failures.add("XML root element has no column mappings");
            }
        }

        if (!type.equals("XML")) {
            // Must have JSON mapping
            JsonDocumentModel jdm = mapping.getJsonDocumentModel();
            if (jdm == null || jdm.getRoot() == null) {
                failures.add("JSON document model has no root element");
            } else if (isBlank(jdm.getRoot().getSourceTable())) {
                failures.add("JSON root element has no source table");
            } else if (jdm.getRoot().getColumns() == null || jdm.getRoot().getColumns().isEmpty()) {
                failures.add("JSON root element has no column mappings");
            }
        }

        if (failures.isEmpty()) {
            return ValidationCheck.pass("MAPPING_COMPLETENESS", CAT_MAPPING, "Mapping completeness");
        }
        return ValidationCheck.fail("MAPPING_COMPLETENESS", CAT_MAPPING,
                "Mapping completeness", String.join("; ", failures),
                "Complete the document mapping in the Document Model view.");
    }

    private ValidationCheck checkParentRefIntegrity(Project project) {
        ProjectMapping mapping = project.getMapping();
        if (mapping == null) {
            return ValidationCheck.pass("PARENTREF_INTEGRITY", CAT_MAPPING, "Parent reference integrity");
        }

        Set<String> allIds = collectAllMappingIds(mapping);
        List<String> broken = new ArrayList<>();

        // XML child mappings
        if (mapping.getDocumentModel() != null && mapping.getDocumentModel().getElements() != null) {
            for (XmlTableMapping el : mapping.getDocumentModel().getElements()) {
                if (!isBlank(el.getParentRef()) && !allIds.contains(el.getParentRef())) {
                    broken.add("XML '" + el.getXmlName() + "' → unknown parentRef " + el.getParentRef());
                }
            }
        }

        // JSON child mappings
        if (mapping.getJsonDocumentModel() != null && mapping.getJsonDocumentModel().getElements() != null) {
            for (JsonTableMapping el : mapping.getJsonDocumentModel().getElements()) {
                if (!isBlank(el.getParentRef()) && !allIds.contains(el.getParentRef())) {
                    broken.add("JSON '" + el.getJsonName() + "' → unknown parentRef " + el.getParentRef());
                }
            }
        }

        if (broken.isEmpty()) {
            return ValidationCheck.pass("PARENTREF_INTEGRITY", CAT_MAPPING, "Parent reference integrity");
        }
        return ValidationCheck.fail("PARENTREF_INTEGRITY", CAT_MAPPING,
                "Parent reference integrity", String.join("; ", broken),
                "Re-save the document mapping to regenerate parent references.");
    }

    private ValidationCheck checkNamespaceDeclarations(Project project) {
        ProjectMapping mapping = project.getMapping();
        if (mapping == null || mapping.getDocumentModel() == null) {
            return ValidationCheck.pass("NAMESPACE_DECLARATIONS", CAT_MAPPING, "Namespace declarations");
        }

        Set<String> declaredPrefixes = mapping.getNamespaces() == null ? Set.of() :
                mapping.getNamespaces().stream()
                        .map(ns -> ns.getPrefix())
                        .filter(p -> !isBlank(p))
                        .collect(Collectors.toSet());

        List<String> undeclared = new ArrayList<>();

        Stream<XmlTableMapping> allXml = allXmlMappings(mapping);
        allXml.forEach(m -> {
            if (!isBlank(m.getNamespacePrefix()) && !declaredPrefixes.contains(m.getNamespacePrefix())) {
                undeclared.add("'" + m.getNamespacePrefix() + "' on element '" + m.getXmlName() + "'");
            }
        });

        if (undeclared.isEmpty()) {
            return ValidationCheck.pass("NAMESPACE_DECLARATIONS", CAT_MAPPING, "Namespace declarations");
        }
        return ValidationCheck.fail("NAMESPACE_DECLARATIONS", CAT_MAPPING,
                "Namespace declarations",
                "Undeclared prefixes: " + String.join(", ", undeclared),
                "Declare missing namespaces in the Namespace Manager.");
    }

    // ── Security check ────────────────────────────────────────────────────────

    private ValidationCheck checkSecurityRoles(MarkLogicSecurityConfig effectiveSecurity,
                                               SavedMarkLogicConnection mlConn) {
        if (effectiveSecurity == null
                || effectiveSecurity.getPermissions() == null
                || effectiveSecurity.getPermissions().isEmpty()) {
            return ValidationCheck.pass("SECURITY_ROLES", CAT_SECURITY, "Security role definitions");
        }

        try {
            String plainPassword = passwordEncryptionService.decrypt(mlConn.getConnection().getPassword());
            RoleValidationResult result = markLogicSecurityService.validateRoles(
                    effectiveSecurity, mlConn.getConnection(), plainPassword);

            if (!result.missingRoles().isEmpty()) {
                return ValidationCheck.fail("SECURITY_ROLES", CAT_SECURITY,
                        "Security role definitions",
                        "Missing roles: " + String.join(", ", result.missingRoles()),
                        "Create the missing roles in MarkLogic Admin UI before migrating.");
            }
            if (!result.unvalidatedRoles().isEmpty()) {
                return ValidationCheck.warn("SECURITY_ROLES", CAT_SECURITY,
                        "Security role definitions",
                        "Could not verify roles (management API unreachable): " + String.join(", ", result.unvalidatedRoles()),
                        "Ensure the MarkLogic Management API (port 8002) is accessible if you want role verification.");
            }
            return ValidationCheck.pass("SECURITY_ROLES", CAT_SECURITY, "Security role definitions");
        } catch (Exception e) {
            return ValidationCheck.warn("SECURITY_ROLES", CAT_SECURITY,
                    "Security role definitions",
                    "Role validation skipped: " + e.getMessage(),
                    "Ensure the MarkLogic Management API (port 8002) is accessible.");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Set<String> collectAllMappingIds(ProjectMapping mapping) {
        Stream<String> xmlIds = Stream.empty();
        if (mapping.getDocumentModel() != null) {
            xmlIds = allXmlMappings(mapping).map(XmlTableMapping::getId).filter(id -> !isBlank(id));
        }
        Stream<String> jsonIds = Stream.empty();
        if (mapping.getJsonDocumentModel() != null) {
            jsonIds = allJsonMappings(mapping).map(JsonTableMapping::getId).filter(id -> !isBlank(id));
        }
        return Stream.concat(xmlIds, jsonIds).collect(Collectors.toSet());
    }

    private Stream<XmlTableMapping> allXmlMappings(ProjectMapping mapping) {
        if (mapping.getDocumentModel() == null) return Stream.empty();
        DocumentModel dm = mapping.getDocumentModel();
        Stream<XmlTableMapping> root = dm.getRoot() != null ? Stream.of(dm.getRoot()) : Stream.empty();
        Stream<XmlTableMapping> elements = dm.getElements() != null ? dm.getElements().stream() : Stream.empty();
        return Stream.concat(root, elements);
    }

    private Stream<JsonTableMapping> allJsonMappings(ProjectMapping mapping) {
        if (mapping.getJsonDocumentModel() == null) return Stream.empty();
        JsonDocumentModel jdm = mapping.getJsonDocumentModel();
        Stream<JsonTableMapping> root = jdm.getRoot() != null ? Stream.of(jdm.getRoot()) : Stream.empty();
        Stream<JsonTableMapping> elements = jdm.getElements() != null ? jdm.getElements().stream() : Stream.empty();
        return Stream.concat(root, elements);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
