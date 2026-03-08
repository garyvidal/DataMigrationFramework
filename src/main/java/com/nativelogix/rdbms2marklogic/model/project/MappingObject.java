package com.nativelogix.rdbms2marklogic.model.project;

import lombok.Data;

import java.util.List;

@Data
public class MappingObject {
    String tableId;
    String documentId;
    String path;
    MappingSettings settings;
    String condition;
    List<Mapping> fields;
    List<CalculatedField> calculatedFields;
}
