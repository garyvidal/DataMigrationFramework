package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

@Data
public class Source {
    String name;
    boolean isPrimaryKey;
    SourceType type;
}
