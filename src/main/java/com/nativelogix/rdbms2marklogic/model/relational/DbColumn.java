package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

import java.sql.JDBCType;

@Data
public class DbColumn {
    String name;
    String type;
    int position;
    boolean isSequence;
    boolean isPrimaryKey;
    DbColumnType columnType;
    DbForeignKey foreignKey;
}
