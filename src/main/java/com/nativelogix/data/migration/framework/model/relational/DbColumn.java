package com.nativelogix.data.migration.framework.model.relational;

import lombok.Data;

@Data
public class DbColumn {
    String name;
    String fullName;
    String type;
    int position;
    boolean isSequence;
    boolean isPrimaryKey;
    DbColumnType columnType;
    DbForeignKey foreignKey;
}
