package com.nativelogix.data.migration.framework.model.relational;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DbTable {
    String tableName;
    String fullName;
    String schema;
    Map<String,DbColumn> columns;
    List<DbRelationship> relationships;
    /** Optional SQL WHERE clause (without the WHERE keyword) to filter rows from this table during migration. */
    String whereClause;
}
