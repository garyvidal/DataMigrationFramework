package com.nativelogix.rdbms2marklogic.model.requests;

import com.nativelogix.rdbms2marklogic.model.Connection;
import lombok.Data;

@Data
public class SchemaAnalysisRequest {
    Connection connection;
    boolean includeTables;
    boolean includeColumns;
    boolean includeRelationships;
    boolean includeViews;
    boolean includeProcedures;
}
