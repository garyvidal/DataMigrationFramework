package com.nativelogix.rdbms2marklogic.model.project;

import com.nativelogix.rdbms2marklogic.model.diagrams.DiagramContainer;
import com.nativelogix.rdbms2marklogic.model.relational.DbSchema;
import lombok.Data;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
public class Project {
    String id;
    String name;
    String version;
    String connectionName;
    OffsetDateTime created;
    OffsetDateTime modified;
    Map<String, DbSchema> schemas;
    ProjectMapping mapping;
    List<DiagramContainer> diagrams;
    ProjectSettings settings;
    List<SyntheticJoin> syntheticJoins;
}
