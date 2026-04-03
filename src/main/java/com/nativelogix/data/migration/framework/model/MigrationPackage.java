package com.nativelogix.data.migration.framework.model;

import com.nativelogix.data.migration.framework.model.project.Project;
import lombok.Data;

import java.time.OffsetDateTime;

/**
 * Portable export bundle containing a project definition and its associated
 * database/MarkLogic connection configurations (passwords excluded).
 * Can be used to share, back up, or deploy migration configurations
 * via the UI or the CLI {@code --package} flag.
 */
@Data
public class MigrationPackage {

    /** Format version for forward-compatibility. */
    private String packageVersion = "1.0";

    /** When this package was exported. */
    private OffsetDateTime exportedAt;

    /** Full project definition. */
    private Project project;

    /**
     * Source relational-database connection (password field is always null in exports).
     * May be null if no source connection was selected at export time.
     */
    private SavedConnection sourceConnection;

    /**
     * Target MarkLogic connection (password field is always null in exports).
     * May be null if no MarkLogic connection was selected at export time.
     */
    private SavedMarkLogicConnection marklogicConnection;
}
