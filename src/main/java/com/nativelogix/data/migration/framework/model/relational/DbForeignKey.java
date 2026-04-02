package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

@Data
public class DbForeignKey {
    String name;
    String fullName;
    String table;
    String column;
    String cardinality;
    String synthetic;
}
