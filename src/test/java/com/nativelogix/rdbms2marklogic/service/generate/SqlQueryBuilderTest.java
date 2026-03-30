package com.nativelogix.rdbms2marklogic.service.generate;

import com.nativelogix.rdbms2marklogic.model.Connection.ConnectionType;
import com.nativelogix.rdbms2marklogic.model.project.XmlTableMapping;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlQueryBuilderTest {

    private final SqlQueryBuilder builder = new SqlQueryBuilder();

    /**
     * Tests that qualifiedTable() produces the correct FROM clause for each DB type.
     *
     * Key case: SQL Server schema names come from SchemaCrawler as "database.schema"
     * (e.g. "AdventureWorks2022.HumanResources"). Each dot-separated part must be
     * quoted individually — not as a single identifier — to produce valid SQL.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("qualifiedTableCases")
    void qualifiedTableIsCorrectForEachDatabase(
            String description,
            ConnectionType dbType,
            String schema,
            String table,
            String expectedFromClause
    ) {
        XmlTableMapping mapping = new XmlTableMapping();
        mapping.setSourceSchema(schema);
        mapping.setSourceTable(table);

        String sql = builder.buildRootQuery(mapping, dbType, 10);

        assertTrue(sql.contains(expectedFromClause),
                "Expected SQL to contain [%s] but got: %s".formatted(expectedFromClause, sql));
    }

    static Stream<Arguments> qualifiedTableCases() {
        return Stream.of(
            // PostgreSQL — schema is a simple identifier, no dots
            Arguments.of("PostgreSQL simple schema",
                ConnectionType.Postgres, "public", "employee",
                "\"public\".\"employee\""),

            // PostgreSQL — no schema provided
            Arguments.of("PostgreSQL no schema",
                ConnectionType.Postgres, null, "employee",
                "\"employee\""),

            // MySQL — schema = database name, no dots
            Arguments.of("MySQL database-as-schema",
                ConnectionType.MySql, "mydb", "users",
                "\"mydb\".\"users\""),

            // Oracle — schema = user/owner, uppercase, no dots
            Arguments.of("Oracle schema",
                ConnectionType.Oracle, "HR", "EMPLOYEES",
                "\"HR\".\"EMPLOYEES\""),

            // SQL Server — simple schema (dbo), no database prefix
            Arguments.of("SQL Server simple dbo schema",
                ConnectionType.SqlServer, "dbo", "Employee",
                "\"dbo\".\"Employee\""),

            // SQL Server — SchemaCrawler qualifies as "database.schema"
            // Must NOT produce ["AdventureWorks2022.HumanResources"]."Employee"
            // Must produce     "AdventureWorks2022"."HumanResources"."Employee"
            Arguments.of("SQL Server database-qualified schema",
                ConnectionType.SqlServer, "AdventureWorks2022.HumanResources", "Employee",
                "\"AdventureWorks2022\".\"HumanResources\".\"Employee\""),

            // SQL Server — another common schema (Sales)
            Arguments.of("SQL Server Sales schema",
                ConnectionType.SqlServer, "AdventureWorks2022.Sales", "SalesOrderHeader",
                "\"AdventureWorks2022\".\"Sales\".\"SalesOrderHeader\""),

            // Identifiers containing double-quote characters are escaped
            Arguments.of("Schema with double-quote in name",
                ConnectionType.Postgres, "my\"schema", "my\"table",
                "\"my\"\"schema\".\"my\"\"table\"")
        );
    }
}
