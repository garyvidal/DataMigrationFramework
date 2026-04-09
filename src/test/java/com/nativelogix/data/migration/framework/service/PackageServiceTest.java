package com.nativelogix.data.migration.framework.service;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.ImportResult;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.model.MigrationPackage;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.repository.ConnectionRepository;
import com.nativelogix.data.migration.framework.repository.MarkLogicConnectionRepository;
import com.nativelogix.data.migration.framework.repository.ProjectRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
class PackageServiceTest {

    @Mock ProjectRepository projectRepository;
    @Mock ConnectionRepository connectionRepository;
    @Mock MarkLogicConnectionRepository markLogicConnectionRepository;

    @InjectMocks PackageService service;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Project project(String id, String name) {
        Project p = new Project();
        p.setId(id);
        p.setName(name);
        return p;
    }

    private SavedConnection savedConn(String id, String name, String password) {
        Connection conn = new Connection();
        conn.setUrl("localhost");
        conn.setUserName("user");
        conn.setPassword(password);
        return new SavedConnection(id, name, null, conn);
    }

    private SavedMarkLogicConnection savedMlConn(String id, String name, String password) {
        MarkLogicConnection conn = new MarkLogicConnection();
        conn.setHost("ml-host");
        conn.setUsername("admin");
        conn.setPassword(password);
        SavedMarkLogicConnection ml = new SavedMarkLogicConnection();
        ml.setId(id);
        ml.setName(name);
        ml.setConnection(conn);
        return ml;
    }

    // ── exportPackage — project ───────────────────────────────────────────────

    @Test
    void exportPackage_projectNotFound_throwsIllegalArgument() {
        when(projectRepository.findById("bad")).thenReturn(Optional.empty());
        assertThrows(IllegalArgumentException.class,
                () -> service.exportPackage("bad", null, null));
    }

    @Test
    void exportPackage_includesProject() {
        when(projectRepository.findById("p1")).thenReturn(Optional.of(project("p1", "MyProject")));
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = service.exportPackage("p1", null, null);

        assertNotNull(pkg.getProject());
        assertEquals("p1", pkg.getProject().getId());
    }

    // ── exportPackage — password stripping ───────────────────────────────────

    @Test
    void exportPackage_sourceConnectionPasswordIsStripped() {
        when(projectRepository.findById("p1")).thenReturn(Optional.of(project("p1", "P")));
        when(connectionRepository.findAll())
                .thenReturn(List.of(savedConn("src-1", "my-src", "secret")));
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = service.exportPackage("p1", "src-1", null);

        assertNotNull(pkg.getSourceConnection());
        assertNull(pkg.getSourceConnection().getConnection().getPassword(),
                "Password must be null in export");
    }

    @Test
    void exportPackage_marklogicConnectionPasswordIsStripped() {
        when(projectRepository.findById("p1")).thenReturn(Optional.of(project("p1", "P")));
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll())
                .thenReturn(List.of(savedMlConn("ml-1", "my-ml", "mlsecret")));

        MigrationPackage pkg = service.exportPackage("p1", null, "ml-1");

        assertNotNull(pkg.getMarklogicConnection());
        assertNull(pkg.getMarklogicConnection().getConnection().getPassword(),
                "Password must be null in export");
    }

    @Test
    void exportPackage_connectionMatchedByName() {
        when(projectRepository.findById("p1")).thenReturn(Optional.of(project("p1", "P")));
        when(connectionRepository.findAll())
                .thenReturn(List.of(savedConn("src-id", "my-source", "pw")));
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = service.exportPackage("p1", "my-source", null);

        assertNotNull(pkg.getSourceConnection());
        assertEquals("src-id", pkg.getSourceConnection().getId());
    }

    @Test
    void exportPackage_nullConnectionIds_connectionsAreNull() {
        when(projectRepository.findById("p1")).thenReturn(Optional.of(project("p1", "P")));

        MigrationPackage pkg = service.exportPackage("p1", null, null);

        assertNull(pkg.getSourceConnection());
        assertNull(pkg.getMarklogicConnection());
        verifyNoInteractions(connectionRepository, markLogicConnectionRepository);
    }

    @Test
    void exportPackage_exportedAtIsSet() {
        when(projectRepository.findById("p1")).thenReturn(Optional.of(project("p1", "P")));

        MigrationPackage pkg = service.exportPackage("p1", null, null);

        assertNotNull(pkg.getExportedAt());
    }

    // ── importPackage — project ───────────────────────────────────────────────

    @Test
    void importPackage_newProject_isSaved() {
        when(projectRepository.exists("p1")).thenReturn(false);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "NewProject"));

        ImportResult result = service.importPackage(pkg, null, null);

        assertTrue(result.isProjectCreated());
        verify(projectRepository).save(eq("p1"), any(Project.class));
    }

    @Test
    void importPackage_existingProject_isNotSavedAgain() {
        when(projectRepository.exists("p1")).thenReturn(true);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "ExistingProject"));

        ImportResult result = service.importPackage(pkg, null, null);

        assertFalse(result.isProjectCreated());
        verify(projectRepository, never()).save(anyString(), any());
    }

    // ── importPackage — source connection ────────────────────────────────────

    @Test
    void importPackage_newSourceConnection_isSaved() {
        when(projectRepository.exists(anyString())).thenReturn(false);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setSourceConnection(savedConn("src-1", "my-src", null));

        ImportResult result = service.importPackage(pkg, "supplied-pw", null);

        assertTrue(result.isSourceConnectionCreated());
        verify(connectionRepository).save(any(SavedConnection.class));
    }

    @Test
    void importPackage_newSourceConnection_passwordApplied() {
        when(projectRepository.exists(anyString())).thenReturn(false);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        SavedConnection conn = savedConn("src-1", "my-src", null);
        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setSourceConnection(conn);

        service.importPackage(pkg, "supplied-pw", null);

        ArgumentCaptor<SavedConnection> captor = ArgumentCaptor.forClass(SavedConnection.class);
        verify(connectionRepository).save(captor.capture());
        assertEquals("supplied-pw", captor.getValue().getConnection().getPassword());
    }

    @Test
    void importPackage_newSourceConnectionWithoutPassword_addsWarning() {
        when(projectRepository.exists(anyString())).thenReturn(false);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setSourceConnection(savedConn("src-1", "my-src", null));

        ImportResult result = service.importPackage(pkg, null, null);

        assertFalse(result.getWarnings().isEmpty());
        assertTrue(result.getWarnings().get(0).contains("my-src"));
    }

    @Test
    void importPackage_existingSourceConnection_notSavedAgain() {
        when(projectRepository.exists(anyString())).thenReturn(true);
        when(connectionRepository.findAll())
                .thenReturn(List.of(savedConn("src-1", "my-src", "existing-pw")));
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setSourceConnection(savedConn("src-1", "my-src", null));

        ImportResult result = service.importPackage(pkg, null, null);

        assertFalse(result.isSourceConnectionCreated());
        verify(connectionRepository, never()).save(argThat(
                c -> c.getConnection() != null && c.getConnection().getPassword() == null));
    }

    @Test
    void importPackage_existingSourceConnection_passwordUpdatedWhenSupplied() {
        SavedConnection existing = savedConn("src-1", "my-src", "old-pw");
        when(projectRepository.exists(anyString())).thenReturn(true);
        when(connectionRepository.findAll()).thenReturn(List.of(existing));
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setSourceConnection(savedConn("src-1", "my-src", null));

        service.importPackage(pkg, "new-pw", null);

        // The existing connection object is updated in-place and re-saved
        verify(connectionRepository).save(argThat(
                c -> "new-pw".equals(c.getConnection().getPassword())));
    }

    // ── importPackage — MarkLogic connection ──────────────────────────────────

    @Test
    void importPackage_newMarklogicConnection_isSaved() {
        when(projectRepository.exists(anyString())).thenReturn(false);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setMarklogicConnection(savedMlConn("ml-1", "my-ml", null));

        ImportResult result = service.importPackage(pkg, null, "ml-pw");

        assertTrue(result.isMarklogicConnectionCreated());
        verify(markLogicConnectionRepository).save(any(SavedMarkLogicConnection.class));
    }

    @Test
    void importPackage_newMarklogicConnectionWithoutPassword_addsWarning() {
        when(projectRepository.exists(anyString())).thenReturn(false);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of());

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setMarklogicConnection(savedMlConn("ml-1", "my-ml", null));

        ImportResult result = service.importPackage(pkg, null, null);

        assertFalse(result.getWarnings().isEmpty());
        assertTrue(result.getWarnings().stream().anyMatch(w -> w.contains("my-ml")));
    }

    @Test
    void importPackage_existingMarklogicConnection_passwordUpdatedWhenSupplied() {
        SavedMarkLogicConnection existing = savedMlConn("ml-1", "my-ml", "old-ml-pw");
        when(projectRepository.exists(anyString())).thenReturn(true);
        when(connectionRepository.findAll()).thenReturn(List.of());
        when(markLogicConnectionRepository.findAll()).thenReturn(List.of(existing));

        MigrationPackage pkg = new MigrationPackage();
        pkg.setProject(project("p1", "P"));
        pkg.setMarklogicConnection(savedMlConn("ml-1", "my-ml", null));

        service.importPackage(pkg, null, "new-ml-pw");

        verify(markLogicConnectionRepository).save(argThat(
                c -> "new-ml-pw".equals(c.getConnection().getPassword())));
    }

    // ── importPackage — empty package ─────────────────────────────────────────

    @Test
    void importPackage_noProject_nothingSaved() {
        MigrationPackage pkg = new MigrationPackage();

        ImportResult result = service.importPackage(pkg, null, null);

        assertNull(result.getProjectId());
        verifyNoInteractions(projectRepository, connectionRepository, markLogicConnectionRepository);
    }
}
