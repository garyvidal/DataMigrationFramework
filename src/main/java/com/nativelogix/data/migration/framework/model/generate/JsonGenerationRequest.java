package com.nativelogix.data.migration.framework.model.generate;

import lombok.Data;

@Data
public class JsonGenerationRequest {
    /** Max number of root-level documents to return. Defaults to 10. */
    int limit = 10;
}
