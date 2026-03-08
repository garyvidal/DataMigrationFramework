package com.nativelogix.rdbms2marklogic.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SavedConnection {
    private String name;
    private Connection connection;
}
