package com.nativelogix.rdbms2marklogic.model.diagrams;

import lombok.Data;

@Data
public class Node {
    String id;
    NodeType type;
    int x;
    int y;
    int width;
    int height;
    boolean collapsed;
}
