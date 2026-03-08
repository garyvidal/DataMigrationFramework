package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

@Data
public class DbColumnType {
    String columnType;
    long precision;
    long scale;
}
