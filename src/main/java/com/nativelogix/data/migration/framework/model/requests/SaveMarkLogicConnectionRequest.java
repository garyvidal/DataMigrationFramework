package com.nativelogix.data.migration.framework.model.requests;

import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import lombok.Data;

@Data
public class SaveMarkLogicConnectionRequest {
    private String id;
    private String name;
    private MarkLogicConnection connection;
}
