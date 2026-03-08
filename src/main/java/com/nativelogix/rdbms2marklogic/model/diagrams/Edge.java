package com.nativelogix.rdbms2marklogic.model.diagrams;

import lombok.Data;

@Data
public class Edge {
    String id;
    String source;
    String target;
    Marker startMarker;
    Marker endMarker;
}
