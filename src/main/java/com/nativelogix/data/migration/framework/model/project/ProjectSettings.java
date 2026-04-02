package com.nativelogix.rdbms2marklogic.model.project;

import com.nativelogix.rdbms2marklogic.model.diagrams.ConnectionLineType;
import lombok.Data;

@Data
public class ProjectSettings {
    NamingCase defaultCasing;
    ConnectionLineType connectionLineType;
}
