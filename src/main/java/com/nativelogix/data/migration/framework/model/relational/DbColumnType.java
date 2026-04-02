package com.nativelogix.data.migration.framework.model.relational;

import lombok.Data;

@Data
public class DbColumnType {
    String columnType;
    long precision;
    long scale;
}
