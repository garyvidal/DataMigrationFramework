package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

import java.util.List;

@Data
public class DocumentModel {
    XmlTableMapping root;
    List<XmlTableMapping> elements;
}
