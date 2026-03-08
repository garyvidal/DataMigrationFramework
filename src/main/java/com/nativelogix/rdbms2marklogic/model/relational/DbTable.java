package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DbTable {
    String tableName;
    String schema;
    Map<String,DbColumn> columns;
    List<DbRelationship> relationships;
}
