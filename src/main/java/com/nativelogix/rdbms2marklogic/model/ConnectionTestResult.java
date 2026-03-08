package com.nativelogix.rdbms2marklogic.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConnectionTestResult {
    private boolean success;
    private String message;
}
