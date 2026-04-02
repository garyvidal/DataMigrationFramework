package com.nativelogix.data.migration.framework.model.diagrams;

import lombok.Data;

import java.util.List;

@Data
public class DiagramContainer {
    String name;
    List<DiagramTab> tabs;
}
