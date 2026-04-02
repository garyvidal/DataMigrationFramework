package com.nativelogix.data.migration.framework.model.relational;

import lombok.Data;

import java.util.Map;

@Data
public class DbSchema {
   String name;
   String fullName;
   Map<String, DbTable> tables;
}
