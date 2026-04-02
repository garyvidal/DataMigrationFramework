package com.nativelogix.data.migration.framework.model.requests;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.ConnectionEnvironment;
import lombok.Data;

@Data
public class SaveConnectionRequest {
    private String id;
    private String name;
    private ConnectionEnvironment environment;
    private Connection connection;
}
