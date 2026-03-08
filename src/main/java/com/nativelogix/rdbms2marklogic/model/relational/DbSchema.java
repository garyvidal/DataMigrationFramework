package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

import java.util.Map;

@Data
public class DbSchema {
   String name;
   Map<String, DbTable> tables;
}
