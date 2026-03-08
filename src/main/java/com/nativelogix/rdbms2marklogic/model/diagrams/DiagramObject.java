package com.nativelogix.rdbms2marklogic.model.diagrams;

import lombok.Data;

import java.util.List;

@Data
public class DiagramObject {
    List<Node> nodes;
    List<Edge> edges;
}
