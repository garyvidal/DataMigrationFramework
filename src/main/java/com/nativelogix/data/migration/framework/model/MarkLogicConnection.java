package com.nativelogix.data.migration.framework.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarkLogicConnection {
    /** Hostname or IP address of the MarkLogic server. */
    private String host;

    /** App-server port (e.g. 8000). */
    private Integer port;

    /** Target database name; if null/blank the app-server's default database is used. */
    private String database;

    private String username;

    /** Plaintext when sending to the backend; null/undefined when received (never returned). */
    private String password;

    /** HTTP authentication scheme: "digest" (default) or "basic". */
    private String authType;

    /** Whether to use SSL/TLS for the connection. */
    private Boolean useSSL;
}
