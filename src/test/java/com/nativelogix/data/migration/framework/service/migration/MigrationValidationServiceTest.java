package com.nativelogix.data.migration.framework.service.migration;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.ConnectionTestResult;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicPermission;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import com.nativelogix.data.migration.framework.model.marklogic.RoleValidationResult;
import com.nativelogix.data.migration.framework.model.migration.MigrationRequest;
import com.nativelogix.data.migration.framework.model.project.DocumentModel;
import com.nativelogix.data.migration.framework.model.project.JsonDocumentModel;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.project.ProjectMapping;
import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlNamespace;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import com.nativelogix.data.migration.framework.model.validation.CheckStatus;
import com.nativelogix.data.migration.framework.model.validation.ValidationCheck;
import com.nativelogix.data.migration.framework.model.validation.ValidationReport;
import com.nativelogix.data.migration.framework.repository.FileSystemProjectRepository;
import com.nativelogix.data.migration.framework.service.JDBCConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicConnectionService;
import com.nativelogix.data.migration.framework.service.MarkLogicSecurityService;
import com.nativelogix.data.migration.framework.service.PasswordEncryptionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MigrationValidationServiceTest {

    @Mock FileSystemProjectRepository projectRepository;
    @Mock JDBCConnectionService jdbcConnectionService;
    @Mock MarkLogicConnectionService markLogicConnectionService;
    @Mock MarkLogicSecurityService markLogicSecurityService;
    @Mock PasswordEncryptionService passwordEncryptionService;

    @InjectMocks
    MigrationValidationService service;

    // ── Shared fixtures ───────────────────────────────────────────────────────

    private Project minimalXmlProject() {
        XmlColumnMapping col = new XmlColumnMapping();
        col.setXmlName("Id");
        col.setMappingType("Element");

        XmlTableMapping root = new XmlTableMapping();
        root.setId("root-id");
        root.setXmlName("Employee");
        root.setSourceTable("employee");
        root.setMappingType("RootElement");
        root.setColumns(List.of(col));

        DocumentModel dm = new DocumentModel();
        dm.setRoot(root);

        ProjectMapping mapping = new ProjectMapping();
        mapping.setMappingType("XML");
        mapping.setDocumentModel(dm);

        Project project = new Project();
        project.setId("proj-1");
        project.setConnectionName("my-db");
        project.setMapping(mapping);
        return project;
    }

    private SavedConnection savedConnection(String id, String name) {
        SavedConnection sc = new SavedConnection();
        sc.setId(id);
        sc.setName(name);
        sc.setConnection(new Connection());
        return sc;
    }

    private SavedMarkLogicConnection savedMlConnection(String id, String name) {
        SavedMarkLogicConnection ml = new SavedMarkLogicConnection();
        ml.setId(id);
        ml.setName(name);
        MarkLogicConnection conn = new MarkLogicConnection();
        conn.setPassword("encrypted-pw");
        ml.setConnection(conn);
        return ml;
    }

    private MigrationRequest request(String projectId, String sourceConnId, String mlConnId) {
        MigrationRequest req = new MigrationRequest();
        req.setProjectId(projectId);
        req.setSourceConnectionId(sourceConnId);
        req.setMarklogicConnectionId(mlConnId);
        return req;
    }

    @BeforeEach
    void stubDefaults() {
        // Default: project found, connections resolved, both reachable
        when(projectRepository.findById("proj-1"))
                .thenReturn(Optional.of(minimalXmlProject()));
        when(jdbcConnectionService.getAllConnections())
                .thenReturn(List.of(savedConnection("src-1", "my-src")));
        when(jdbcConnectionService.getConnection("my-db"))
                .thenReturn(Optional.of(savedConnection("src-1", "my-db")));
        when(markLogicConnectionService.getAllConnections())
                .thenReturn(List.of(savedMlConnection("ml-1", "my-ml")));
        when(jdbcConnectionService.testConnection(any()))
                .thenReturn(new ConnectionTestResult(true, "OK"));
        when(markLogicConnectionService.testConnection(any()))
                .thenReturn(new ConnectionTestResult(true, "OK"));
        when(markLogicSecurityService.mergeConfigs(any(), any()))
                .thenReturn(null);
    }

    // ── Project resolution ────────────────────────────────────────────────────

    @Test
    void validate_projectNotFound_reportsFail() {
        when(projectRepository.findById("bad-id")).thenReturn(Optional.empty());
        MigrationRequest req = request("bad-id", "src-1", "ml-1");

        ValidationReport report = service.validate(req);

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "PROJECT_RESOLUTION");
        assertEquals(CheckStatus.FAIL, check.status());
    }

    // ── Source connection resolution ──────────────────────────────────────────

    @Test
    void validate_sourceConnectionNotFound_reportsFail() {
        when(jdbcConnectionService.getAllConnections()).thenReturn(List.of());
        when(jdbcConnectionService.getConnection(anyString())).thenReturn(Optional.empty());

        ValidationReport report = service.validate(request("proj-1", null, "ml-1"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "SOURCE_RESOLUTION");
        assertEquals(CheckStatus.FAIL, check.status());
    }

    @Test
    void validate_sourceConnectionFoundById_passes() {
        MigrationRequest req = request("proj-1", "src-1", "ml-1");

        ValidationReport report = service.validate(req);

        ValidationCheck check = findCheck(report, "SOURCE_CONNECTIVITY");
        assertEquals(CheckStatus.PASS, check.status());
    }

    // ── MarkLogic connection resolution ───────────────────────────────────────

    @Test
    void validate_marklogicConnectionNotFound_reportsFail() {
        when(markLogicConnectionService.getAllConnections()).thenReturn(List.of());

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-missing"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "MARKLOGIC_RESOLUTION");
        assertEquals(CheckStatus.FAIL, check.status());
    }

    // ── Connectivity checks ───────────────────────────────────────────────────

    @Test
    void validate_sourceDbUnreachable_reportsConnectivityFail() {
        when(jdbcConnectionService.testConnection(any()))
                .thenReturn(new ConnectionTestResult(false, "Connection refused"));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "SOURCE_CONNECTIVITY");
        assertEquals(CheckStatus.FAIL, check.status());
        assertTrue(check.detail().contains("Connection refused"));
    }

    @Test
    void validate_marklogicUnreachable_reportsConnectivityFail() {
        when(markLogicConnectionService.testConnection(any()))
                .thenReturn(new ConnectionTestResult(false, "Timeout"));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "MARKLOGIC_CONNECTIVITY");
        assertEquals(CheckStatus.FAIL, check.status());
    }

    @Test
    void validate_bothConnectionsReachable_passesConnectivityChecks() {
        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertEquals(CheckStatus.PASS, findCheck(report, "SOURCE_CONNECTIVITY").status());
        assertEquals(CheckStatus.PASS, findCheck(report, "MARKLOGIC_CONNECTIVITY").status());
    }

    // ── Mapping completeness ──────────────────────────────────────────────────

    @Test
    void validate_noMapping_reportsMappingFail() {
        Project project = minimalXmlProject();
        project.setMapping(null);
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        assertEquals(CheckStatus.FAIL, findCheck(report, "MAPPING_COMPLETENESS").status());
    }

    @Test
    void validate_xmlRootHasNoSourceTable_reportsMappingFail() {
        Project project = minimalXmlProject();
        project.getMapping().getDocumentModel().getRoot().setSourceTable(null);
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        assertEquals(CheckStatus.FAIL, findCheck(report, "MAPPING_COMPLETENESS").status());
    }

    @Test
    void validate_xmlRootHasNoColumns_reportsMappingFail() {
        Project project = minimalXmlProject();
        project.getMapping().getDocumentModel().getRoot().setColumns(List.of());
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        assertEquals(CheckStatus.FAIL, findCheck(report, "MAPPING_COMPLETENESS").status());
    }

    @Test
    void validate_completeXmlMapping_passesMappingCompleteness() {
        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertEquals(CheckStatus.PASS, findCheck(report, "MAPPING_COMPLETENESS").status());
    }

    @Test
    void validate_jsonMappingType_checksJsonModel() {
        Project project = minimalXmlProject();
        project.getMapping().setMappingType("JSON");
        // No JSON model configured
        project.getMapping().setJsonDocumentModel(null);
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        String detail = findCheck(report, "MAPPING_COMPLETENESS").detail();
        assertTrue(detail.contains("JSON"));
    }

    @Test
    void validate_bothMappingType_checksXmlAndJson() {
        Project project = minimalXmlProject();
        project.getMapping().setMappingType("BOTH");
        // JSON model is missing
        project.getMapping().setJsonDocumentModel(null);
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        String detail = findCheck(report, "MAPPING_COMPLETENESS").detail();
        assertTrue(detail.contains("JSON"));
    }

    // ── ParentRef integrity ───────────────────────────────────────────────────

    @Test
    void validate_brokenXmlParentRef_reportsParentRefFail() {
        XmlTableMapping child = new XmlTableMapping();
        child.setId("child-id");
        child.setXmlName("Phone");
        child.setMappingType("InlineElement");
        child.setParentRef("does-not-exist");  // orphaned ref

        Project project = minimalXmlProject();
        project.getMapping().getDocumentModel().setElements(List.of(child));
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "PARENTREF_INTEGRITY");
        assertEquals(CheckStatus.FAIL, check.status());
        assertTrue(check.detail().contains("does-not-exist"));
    }

    @Test
    void validate_validParentRef_passesParentRefIntegrity() {
        XmlTableMapping child = new XmlTableMapping();
        child.setId("child-id");
        child.setXmlName("Phone");
        child.setMappingType("InlineElement");
        child.setParentRef("root-id");  // valid — root has id="root-id"

        Project project = minimalXmlProject();
        project.getMapping().getDocumentModel().setElements(List.of(child));
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertEquals(CheckStatus.PASS, findCheck(report, "PARENTREF_INTEGRITY").status());
    }

    @Test
    void validate_brokenJsonParentRef_reportsParentRefFail() {
        JsonTableMapping jsonRoot = new JsonTableMapping();
        jsonRoot.setId("json-root");
        jsonRoot.setJsonName("record");
        jsonRoot.setSourceTable("employee");
        jsonRoot.setColumns(List.of());

        JsonTableMapping child = new JsonTableMapping();
        child.setId("json-child");
        child.setJsonName("address");
        child.setParentRef("no-such-id");

        JsonDocumentModel jdm = new JsonDocumentModel();
        jdm.setRoot(jsonRoot);
        jdm.setElements(List.of(child));

        Project project = minimalXmlProject();
        project.getMapping().setMappingType("BOTH");
        project.getMapping().setJsonDocumentModel(jdm);
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        assertTrue(findCheck(report, "PARENTREF_INTEGRITY").detail().contains("no-such-id"));
    }

    // ── Namespace declarations ────────────────────────────────────────────────

    @Test
    void validate_undeclaredNamespacePrefix_reportsNamespaceFail() {
        XmlTableMapping root = minimalXmlProject().getMapping().getDocumentModel().getRoot();
        root.setNamespacePrefix("dc");  // used but not declared

        Project project = minimalXmlProject();
        project.getMapping().getDocumentModel().setRoot(root);
        project.getMapping().setNamespaces(List.of());  // no namespaces declared
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "NAMESPACE_DECLARATIONS");
        assertEquals(CheckStatus.FAIL, check.status());
        assertTrue(check.detail().contains("dc"));
    }

    @Test
    void validate_declaredNamespacePrefix_passesNamespaceCheck() {
        XmlNamespace ns = new XmlNamespace();
        ns.setPrefix("dc");
        ns.setUri("http://purl.org/dc/elements/1.1/");

        XmlTableMapping root = minimalXmlProject().getMapping().getDocumentModel().getRoot();
        root.setNamespacePrefix("dc");

        Project project = minimalXmlProject();
        project.getMapping().getDocumentModel().setRoot(root);
        project.getMapping().setNamespaces(List.of(ns));
        when(projectRepository.findById("proj-1")).thenReturn(Optional.of(project));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertEquals(CheckStatus.PASS, findCheck(report, "NAMESPACE_DECLARATIONS").status());
    }

    // ── Security role validation ──────────────────────────────────────────────

    @Test
    void validate_missingSecurityRoles_reportsSecurityFail() throws Exception {
        MarkLogicPermission perm = new MarkLogicPermission();
        perm.setRoleName("data-reader");
        perm.setCapabilities(List.of("read"));

        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setPermissions(List.of(perm));

        when(markLogicSecurityService.mergeConfigs(any(), any())).thenReturn(config);
        when(passwordEncryptionService.decrypt(anyString())).thenReturn("plain-pw");
        when(markLogicSecurityService.validateRoles(any(), any(), anyString()))
                .thenReturn(new RoleValidationResult(false, List.of("data-reader"), List.of()));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertFalse(report.isCanProceed());
        ValidationCheck check = findCheck(report, "SECURITY_ROLES");
        assertEquals(CheckStatus.FAIL, check.status());
        assertTrue(check.detail().contains("data-reader"));
    }

    @Test
    void validate_unverifiableRoles_reportsSecurityWarn() throws Exception {
        MarkLogicPermission perm = new MarkLogicPermission();
        perm.setRoleName("editor");
        perm.setCapabilities(List.of("update"));

        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setPermissions(List.of(perm));

        when(markLogicSecurityService.mergeConfigs(any(), any())).thenReturn(config);
        when(passwordEncryptionService.decrypt(anyString())).thenReturn("plain-pw");
        when(markLogicSecurityService.validateRoles(any(), any(), anyString()))
                .thenReturn(new RoleValidationResult(false, List.of(), List.of("editor")));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        // WARN should not block proceeding
        assertTrue(report.isCanProceed());
        assertTrue(report.isHasWarnings());
        assertEquals(CheckStatus.WARN, findCheck(report, "SECURITY_ROLES").status());
    }

    @Test
    void validate_allRolesValid_passesSecurityCheck() throws Exception {
        MarkLogicPermission perm = new MarkLogicPermission();
        perm.setRoleName("reader");
        perm.setCapabilities(List.of("read"));

        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setPermissions(List.of(perm));

        when(markLogicSecurityService.mergeConfigs(any(), any())).thenReturn(config);
        when(passwordEncryptionService.decrypt(anyString())).thenReturn("plain-pw");
        when(markLogicSecurityService.validateRoles(any(), any(), anyString()))
                .thenReturn(new RoleValidationResult(true, List.of(), List.of()));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertTrue(report.isCanProceed());
        assertEquals(CheckStatus.PASS, findCheck(report, "SECURITY_ROLES").status());
    }

    @Test
    void validate_noSecurityConfig_skipsRoleCheck() {
        when(markLogicSecurityService.mergeConfigs(any(), any())).thenReturn(null);

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        assertEquals(CheckStatus.PASS, findCheck(report, "SECURITY_ROLES").status());
        verify(markLogicSecurityService, never()).validateRoles(any(), any(), any());
    }

    @Test
    void validate_roleValidationThrows_reportsSecurityWarn() throws Exception {
        MarkLogicPermission perm = new MarkLogicPermission();
        perm.setRoleName("reader");
        perm.setCapabilities(List.of("read"));

        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setPermissions(List.of(perm));

        when(markLogicSecurityService.mergeConfigs(any(), any())).thenReturn(config);
        when(passwordEncryptionService.decrypt(anyString())).thenReturn("plain-pw");
        when(markLogicSecurityService.validateRoles(any(), any(), anyString()))
                .thenThrow(new RuntimeException("Management API down"));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));

        // Exception → WARN, not FAIL
        assertTrue(report.isCanProceed());
        assertEquals(CheckStatus.WARN, findCheck(report, "SECURITY_ROLES").status());
    }

    // ── canProceed / hasWarnings ──────────────────────────────────────────────

    @Test
    void validate_allChecksPassing_canProceedIsTrue() {
        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));
        assertTrue(report.isCanProceed());
        assertFalse(report.isHasWarnings());
    }

    @Test
    void validate_anyCheckFailing_canProceedIsFalse() {
        when(jdbcConnectionService.testConnection(any()))
                .thenReturn(new ConnectionTestResult(false, "down"));

        ValidationReport report = service.validate(request("proj-1", "src-1", "ml-1"));
        assertFalse(report.isCanProceed());
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private ValidationCheck findCheck(ValidationReport report, String checkId) {
        return report.getChecks().stream()
                .filter(c -> checkId.equals(c.checkId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Check not found: " + checkId));
    }
}
