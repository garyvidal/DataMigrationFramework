package com.nativelogix.data.migration.framework.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Connection {
    ConnectionType type;
    /** Hostname or IP address */
    String url;
    Integer port;
    String database;
    String userName;
    String password;

    /** When true, use jdbcUri directly instead of building a URL from host/port/database */
    Boolean enterUriManually;
    /** Full JDBC URI, used when enterUriManually is true */
    String jdbcUri;

    /** SQL Server only: "Windows" or "SqlServer" */
    String authentication;

    /** Oracle only: "ServiceName" or "SID" */
    String identifier;
    /** Oracle only: pluggable database name */
    String pdbName;

    /** Postgres only */
    Boolean useSSL;
    /** Postgres only, when useSSL is true: "Prefer" | "Require" | "VerifyCA" | "VerifyFull" */
    String sslMode;

    public enum ConnectionType {
        Postgres,
        MySql,
        SqlServer,
        Oracle
    }
}
