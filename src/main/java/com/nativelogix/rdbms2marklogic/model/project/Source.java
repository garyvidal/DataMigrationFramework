package com.nativelogix.rdbms2marklogic.model.project;

import lombok.Data;

@Data
public class Source {
    String name;
    boolean isPrimaryKey;
    SourceType type;
}
