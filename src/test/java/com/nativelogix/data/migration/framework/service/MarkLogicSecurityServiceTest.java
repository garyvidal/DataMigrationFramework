package com.nativelogix.data.migration.framework.service;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicPermission;
import com.nativelogix.data.migration.framework.model.marklogic.MarkLogicSecurityConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MarkLogicSecurityServiceTest {

    private final MarkLogicSecurityService service = new MarkLogicSecurityService();

    // ── buildMetadataHandle ───────────────────────────────────────────────────

    @Test
    void buildMetadataHandle_nullConfig_returnsNull() {
        assertNull(service.buildMetadataHandle(null));
    }

    @Test
    void buildMetadataHandle_emptyConfig_returnsNull() {
        assertNull(service.buildMetadataHandle(new MarkLogicSecurityConfig()));
    }

    @Test
    void buildMetadataHandle_withCollections_setsCollections() {
        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setCollections(List.of("col-a", "col-b"));

        DocumentMetadataHandle handle = service.buildMetadataHandle(config);

        assertNotNull(handle);
        assertTrue(handle.getCollections().contains("col-a"));
        assertTrue(handle.getCollections().contains("col-b"));
    }

    @Test
    void buildMetadataHandle_withQuality_setsQuality() {
        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setQuality(42);

        DocumentMetadataHandle handle = service.buildMetadataHandle(config);

        assertNotNull(handle);
        assertEquals(42, handle.getQuality());
    }

    @Test
    void buildMetadataHandle_withMetadata_setsMetadataValues() {
        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setMetadata(Map.of("source", "migration", "env", "prod"));

        DocumentMetadataHandle handle = service.buildMetadataHandle(config);

        assertNotNull(handle);
        assertEquals("migration", handle.getMetadataValues().get("source"));
        assertEquals("prod", handle.getMetadataValues().get("env"));
    }

    @Test
    void buildMetadataHandle_withPermissions_addsPermissions() {
        MarkLogicPermission perm = new MarkLogicPermission();
        perm.setRoleName("data-reader");
        perm.setCapabilities(List.of("read"));

        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setPermissions(List.of(perm));

        DocumentMetadataHandle handle = service.buildMetadataHandle(config);

        assertNotNull(handle);
        assertFalse(handle.getPermissions().isEmpty());
    }

    @Test
    void buildMetadataHandle_permissionWithBlankRole_isSkipped() {
        MarkLogicPermission blankRole = new MarkLogicPermission();
        blankRole.setRoleName("  ");
        blankRole.setCapabilities(List.of("read"));

        MarkLogicPermission validRole = new MarkLogicPermission();
        validRole.setRoleName("editor");
        validRole.setCapabilities(List.of("read", "update"));

        MarkLogicSecurityConfig config = new MarkLogicSecurityConfig();
        config.setPermissions(List.of(blankRole, validRole));

        DocumentMetadataHandle handle = service.buildMetadataHandle(config);

        assertNotNull(handle);
        assertEquals(1, handle.getPermissions().size());
    }

    // ── mergeConfigs ──────────────────────────────────────────────────────────

    @Test
    void mergeConfigs_bothNull_returnsNull() {
        assertNull(service.mergeConfigs(null, null));
    }

    @Test
    void mergeConfigs_onlyProject_returnsProject() {
        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setCollections(List.of("proj-col"));

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, null);

        assertSame(project, merged);
    }

    @Test
    void mergeConfigs_onlyJob_returnsJob() {
        MarkLogicSecurityConfig job = new MarkLogicSecurityConfig();
        job.setCollections(List.of("job-col"));

        MarkLogicSecurityConfig merged = service.mergeConfigs(null, job);

        assertSame(job, merged);
    }

    @Test
    void mergeConfigs_collections_areUnioned() {
        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setCollections(List.of("proj-col", "shared-col"));

        MarkLogicSecurityConfig job = new MarkLogicSecurityConfig();
        job.setCollections(List.of("job-col", "shared-col"));

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, job);

        assertNotNull(merged.getCollections());
        assertEquals(3, merged.getCollections().size(), "shared-col should appear only once");
        assertTrue(merged.getCollections().contains("proj-col"));
        assertTrue(merged.getCollections().contains("job-col"));
        assertTrue(merged.getCollections().contains("shared-col"));
    }

    @Test
    void mergeConfigs_jobPermissions_overrideProject() {
        MarkLogicPermission projPerm = new MarkLogicPermission();
        projPerm.setRoleName("reader");
        projPerm.setCapabilities(List.of("read"));

        MarkLogicPermission jobPerm = new MarkLogicPermission();
        jobPerm.setRoleName("writer");
        jobPerm.setCapabilities(List.of("read", "update"));

        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setPermissions(List.of(projPerm));

        MarkLogicSecurityConfig job = new MarkLogicSecurityConfig();
        job.setPermissions(List.of(jobPerm));

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, job);

        assertEquals(1, merged.getPermissions().size());
        assertEquals("writer", merged.getPermissions().get(0).getRoleName());
    }

    @Test
    void mergeConfigs_projectPermissionsUsed_whenJobHasNone() {
        MarkLogicPermission projPerm = new MarkLogicPermission();
        projPerm.setRoleName("reader");
        projPerm.setCapabilities(List.of("read"));

        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setPermissions(List.of(projPerm));

        MarkLogicSecurityConfig job = new MarkLogicSecurityConfig();
        // no permissions on job

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, job);

        assertEquals(1, merged.getPermissions().size());
        assertEquals("reader", merged.getPermissions().get(0).getRoleName());
    }

    @Test
    void mergeConfigs_jobQuality_overridesProject() {
        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setQuality(5);

        MarkLogicSecurityConfig job = new MarkLogicSecurityConfig();
        job.setQuality(99);

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, job);

        assertEquals(99, merged.getQuality());
    }

    @Test
    void mergeConfigs_projectQualityUsed_whenJobHasNone() {
        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setQuality(10);

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, new MarkLogicSecurityConfig());

        assertEquals(10, merged.getQuality());
    }

    @Test
    void mergeConfigs_metadata_mergedWithJobWinning() {
        MarkLogicSecurityConfig project = new MarkLogicSecurityConfig();
        project.setMetadata(Map.of("env", "staging", "owner", "team-a"));

        MarkLogicSecurityConfig job = new MarkLogicSecurityConfig();
        job.setMetadata(Map.of("env", "prod"));  // overrides project "env"

        MarkLogicSecurityConfig merged = service.mergeConfigs(project, job);

        assertNotNull(merged.getMetadata());
        assertEquals("prod", merged.getMetadata().get("env"),    "job should win on duplicate key");
        assertEquals("team-a", merged.getMetadata().get("owner"), "project key should survive");
    }
}
