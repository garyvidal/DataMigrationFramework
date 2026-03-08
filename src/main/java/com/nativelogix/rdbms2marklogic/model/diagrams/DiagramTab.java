package com.nativelogix.rdbms2marklogic.model.diagrams;

import lombok.Data;

@Data
public class DiagramTab {
    String id;
    String name;
    DiagramObject relational;
    DiagramObject document;
}
