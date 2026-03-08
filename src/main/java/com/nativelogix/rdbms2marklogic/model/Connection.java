package com.nativelogix.rdbms2marklogic.model;

import lombok.Data;

@Data
public class Connection {
    ConnectionType type;
    String url;
    Integer port;
    String database;
    String userName;
    String password;
    public enum ConnectionType {
        Postgres,
        MySql,
        SqlServer,
        Oracle
    }
}
