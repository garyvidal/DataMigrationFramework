package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

import java.util.List;

@Data
public class JsonDocumentModel {
    JsonTableMapping root;
    List<JsonTableMapping> elements;
}
