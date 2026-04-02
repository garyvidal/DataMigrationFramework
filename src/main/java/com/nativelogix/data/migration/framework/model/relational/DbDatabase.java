package com.nativelogix.data.migration.framework.model.relational;

import lombok.Data;

import java.util.Map;

@Data
public class DbDatabase {
   String name;
   String fullName;
   Map<String, DbSchema> schemas;
}
