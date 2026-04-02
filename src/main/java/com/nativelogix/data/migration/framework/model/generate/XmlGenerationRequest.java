package com.nativelogix.data.migration.framework.model.generate;

import lombok.Data;

@Data
public class XmlGenerationRequest {
    /** Max number of root-level documents to return. Defaults to 10. */
    int limit = 10;
}
