package com.nativelogix.rdbms2marklogic.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SavedMarkLogicConnection {
    private String id;
    private String name;
    private MarkLogicConnection connection;
}
