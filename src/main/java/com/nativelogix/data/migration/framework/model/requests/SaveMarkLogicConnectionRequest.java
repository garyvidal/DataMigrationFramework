package com.nativelogix.rdbms2marklogic.model.requests;

import com.nativelogix.rdbms2marklogic.model.MarkLogicConnection;
import lombok.Data;

@Data
public class SaveMarkLogicConnectionRequest {
    private String id;
    private String name;
    private MarkLogicConnection connection;
}
