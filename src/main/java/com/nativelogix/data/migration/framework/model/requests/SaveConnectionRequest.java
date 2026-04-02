package com.nativelogix.rdbms2marklogic.model.requests;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.ConnectionEnvironment;
import lombok.Data;

@Data
public class SaveConnectionRequest {
    private String id;
    private String name;
    private ConnectionEnvironment environment;
    private Connection connection;
}
