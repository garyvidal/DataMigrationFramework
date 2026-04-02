package com.nativelogix.data.migration.framework.model.relational;

import lombok.Data;

@Data
public class DbRelationship {
    String fromColumn;
    String toTable;
    String toColumn;
    DbRelationshipType relationshipType;

}
