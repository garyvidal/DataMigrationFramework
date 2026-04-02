package com.nativelogix.data.migration.framework.model.requests;

import com.nativelogix.data.migration.framework.model.Connection;
import lombok.Data;

@Data
public class SchemaAnalysisRequest {
    /** Optional: resolve connection from the repository by id (password never required from client). */
    String connectionId;
    Connection connection;
    boolean includeTables;
    boolean includeColumns;
    boolean includeRelationships;
    boolean includeViews;
    boolean includeProcedures;
}
