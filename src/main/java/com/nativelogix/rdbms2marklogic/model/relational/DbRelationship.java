package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

@Data
public class DbRelationship {
    String fromColumn;
    String toTable;
    String toColumn;
    DbRelationshipType relationshipType;

}
