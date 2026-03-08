package com.nativelogix.rdbms2marklogic.model.relational;

import lombok.Data;

import java.util.Map;

@Data
public class DbDatabase {
   String name;
   Map<String, DbSchema> schemas;
}
