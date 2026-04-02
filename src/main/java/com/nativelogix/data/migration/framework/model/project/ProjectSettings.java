package com.nativelogix.data.migration.framework.model.project;

import com.nativelogix.data.migration.framework.model.diagrams.ConnectionLineType;
import lombok.Data;

@Data
public class ProjectSettings {
    NamingCase defaultCasing;
    ConnectionLineType connectionLineType;
}
