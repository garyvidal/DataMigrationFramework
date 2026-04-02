package com.nativelogix.data.migration.framework.model.diagrams;

import lombok.Data;

@Data
public class Edge {
    String id;
    String source;
    String target;
    Marker startMarker;
    Marker endMarker;
}
