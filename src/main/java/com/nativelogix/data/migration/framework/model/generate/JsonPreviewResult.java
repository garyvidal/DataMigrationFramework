package com.nativelogix.data.migration.framework.model.generate;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class JsonPreviewResult {
    List<String> documents = new ArrayList<>();
    int totalRows;
    List<String> errors = new ArrayList<>();
}
