package com.nativelogix.rdbms2marklogic.model.generate;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class XmlPreviewResult {
    List<String> documents = new ArrayList<>();
    int totalRows;
    List<String> errors = new ArrayList<>();
}
