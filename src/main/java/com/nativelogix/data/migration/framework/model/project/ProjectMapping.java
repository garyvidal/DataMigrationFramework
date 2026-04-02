package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ProjectMapping {
    DocumentModel documentModel;
    JsonDocumentModel jsonDocumentModel;
    /** "XML" (default), "JSON", or "BOTH". */
    String mappingType = "XML";
    /** XML namespace declarations applied to all generated documents. */
    List<XmlNamespace> namespaces = new ArrayList<>();
}
