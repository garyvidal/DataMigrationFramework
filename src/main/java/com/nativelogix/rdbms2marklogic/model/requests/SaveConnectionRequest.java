package com.nativelogix.rdbms2marklogic.model.requests;

import com.nativelogix.rdbms2marklogic.model.Connection;
import lombok.Data;

@Data
public class SaveConnectionRequest {
    private String name;
    private Connection connection;
}
