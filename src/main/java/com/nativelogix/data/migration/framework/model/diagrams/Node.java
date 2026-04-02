package com.nativelogix.data.migration.framework.model.diagrams;

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
