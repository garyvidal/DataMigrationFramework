package com.nativelogix.data.migration.framework.model.diagrams;

import lombok.Data;

@Data
public class DiagramTab {
    String id;
    String name;
    DiagramObject relational;
    DiagramObject document;
}
