package com.nativelogix.rdbms2marklogic.model.requests;

import com.nativelogix.rdbms2marklogic.model.Connection;
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
