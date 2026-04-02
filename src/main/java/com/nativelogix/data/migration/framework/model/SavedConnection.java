package com.nativelogix.data.migration.framework.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SavedConnection {
    private String id;
    private String name;
    private ConnectionEnvironment environment;
    private Connection connection;
}
