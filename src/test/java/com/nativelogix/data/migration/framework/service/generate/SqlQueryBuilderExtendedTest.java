package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.Connection.ConnectionType;
import com.nativelogix.data.migration.framework.model.project.JsonColumnMapping;
import com.nativelogix.data.migration.framework.model.project.JsonTableMapping;
import com.nativelogix.data.migration.framework.model.project.XmlColumnMapping;
import com.nativelogix.data.migration.framework.model.project.XmlTableMapping;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the parts of SqlQueryBuilder not exercised by SqlQueryBuilderTest:
 * child queries, batch queries, paged queries, WHERE clause passthrough,
 * column select list building, and JSON mapping variants.
 */
class SqlQueryBuilderExtendedTest {

    private final SqlQueryBuilder builder = new SqlQueryBuilder();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private XmlTableMapping xmlMapping(String schema, String table, List<XmlColumnMapping> cols) {
        XmlTableMapping m = new XmlTableMapping();
        m.setSourceSchema(schema);
        m.setSourceTable(table);
        m.setColumns(cols);
        return m;
    }

    private XmlColumnMapping xmlCol(String sourceCol) {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setSourceColumn(sourceCol);
        c.setMappingType("Element");
        return c;
    }

    private XmlColumnMapping xmlCustomCol() {
        XmlColumnMapping c = new XmlColumnMapping();
        c.setMappingType("CUSTOM");
        c.setCustomFunction("return 'x';");
        return c;
    }

    private JsonTableMapping jsonMapping(String schema, String table, List<JsonColumnMapping> cols) {
        JsonTableMapping m = new JsonTableMapping();
        m.setSourceSchema(schema);
        m.setSourceTable(table);
        m.setColumns(cols);
        return m;
    }

    private JsonColumnMapping jsonCol(String sourceCol) {
        JsonColumnMapping c = new JsonColumnMapping();
        c.setSourceColumn(sourceCol);
        c.setMappingType("Property");
        return c;
    }

    private JsonColumnMapping jsonCustomCol() {
        JsonColumnMapping c = new JsonColumnMapping();
        c.setMappingType("CUSTOM");
        c.setCustomFunction("return 'x';");
        return c;
    }

    // ── buildRootQuery — WHERE clause passthrough ────────────────────────────

    @Test
    void buildRootQuery_withWhereClause_appendsWhere() {
        XmlTableMapping m = xmlMapping("public", "employee", List.of(xmlCol("id")));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10, "active = true");
        assertTrue(sql.contains("WHERE active = true"));
    }

    @Test
    void buildRootQuery_nullWhereClause_noWhereInSql() {
        XmlTableMapping m = xmlMapping("public", "employee", List.of(xmlCol("id")));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10, null);
        assertFalse(sql.contains("WHERE"));
    }

    @Test
    void buildRootQuery_blankWhereClause_noWhereInSql() {
        XmlTableMapping m = xmlMapping("public", "employee", List.of(xmlCol("id")));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10, "   ");
        assertFalse(sql.contains("WHERE"));
    }

    // ── buildRootQuery — limit syntax per dialect ────────────────────────────

    @ParameterizedTest(name = "{0}")
    @MethodSource("limitSyntaxCases")
    void buildRootQuery_limitSyntaxPerDialect(String desc, ConnectionType dbType, String expectedFragment) {
        XmlTableMapping m = xmlMapping("dbo", "order", List.of(xmlCol("id")));
        String sql = builder.buildRootQuery(m, dbType, 100);
        assertTrue(sql.contains(expectedFragment),
                "Expected [%s] in: %s".formatted(expectedFragment, sql));
    }

    static Stream<Arguments> limitSyntaxCases() {
        return Stream.of(
            Arguments.of("PostgreSQL uses LIMIT",   ConnectionType.Postgres,  "LIMIT 100"),
            Arguments.of("MySQL uses LIMIT",        ConnectionType.MySql,     "LIMIT 100"),
            Arguments.of("SQL Server uses TOP",     ConnectionType.SqlServer, "TOP 100"),
            Arguments.of("Oracle uses FETCH FIRST", ConnectionType.Oracle,    "FETCH FIRST 100 ROWS ONLY")
        );
    }

    // ── buildChildQuery ───────────────────────────────────────────────────────

    @Test
    void buildChildQuery_xml_containsWhereWithPlaceholder() {
        XmlTableMapping m = xmlMapping("public", "address", List.of(xmlCol("id"), xmlCol("city")));
        String sql = builder.buildChildQuery(m, "customer_id");
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"customer_id\" = ?"));
    }

    @Test
    void buildChildQuery_xml_selectsOnlyMappedColumns() {
        XmlTableMapping m = xmlMapping("public", "address",
                List.of(xmlCol("id"), xmlCol("city")));
        String sql = builder.buildChildQuery(m, "customer_id");
        assertTrue(sql.contains("\"id\""));
        assertTrue(sql.contains("\"city\""));
        assertFalse(sql.contains("*"));
    }

    @Test
    void buildChildQuery_json_containsWhereWithPlaceholder() {
        JsonTableMapping m = jsonMapping("public", "address",
                List.of(jsonCol("id"), jsonCol("street")));
        String sql = builder.buildChildQuery(m, "order_id");
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"order_id\" = ?"));
    }

    // ── buildChildBatchQuery ──────────────────────────────────────────────────

    @Test
    void buildChildBatchQuery_xml_containsInClauseWithCorrectPlaceholderCount() {
        XmlTableMapping m = xmlMapping("public", "item", List.of(xmlCol("id")));
        String sql = builder.buildChildBatchQuery(m, "order_id", 5);
        assertTrue(sql.contains("IN ("));
        // 5 placeholders: ?,?,?,?,?
        long count = sql.chars().filter(c -> c == '?').count();
        assertEquals(5, count);
    }

    @Test
    void buildChildBatchQuery_json_containsInClauseWithCorrectPlaceholderCount() {
        JsonTableMapping m = jsonMapping("public", "item", List.of(jsonCol("id")));
        String sql = builder.buildChildBatchQuery(m, "order_id", 3);
        assertTrue(sql.contains("IN ("));
        long count = sql.chars().filter(c -> c == '?').count();
        assertEquals(3, count);
    }

    @Test
    void buildChildBatchQuery_singleItem_onePlaceholder() {
        XmlTableMapping m = xmlMapping("public", "item", List.of(xmlCol("id")));
        String sql = builder.buildChildBatchQuery(m, "fk_id", 1);
        assertTrue(sql.contains("IN (?)"));
    }

    // ── buildPagedRootQuery ───────────────────────────────────────────────────

    @ParameterizedTest(name = "{0}")
    @MethodSource("pagedQueryCases")
    void buildPagedRootQuery_dialectPagination(String desc, ConnectionType dbType,
                                               String mustContain, String mustNotContain) {
        XmlTableMapping m = xmlMapping("public", "employee", List.of(xmlCol("id")));
        String sql = builder.buildPagedRootQuery(m, dbType, null, 100L, 50L);
        assertTrue(sql.contains(mustContain),
                "Expected [%s] in: %s".formatted(mustContain, sql));
        if (mustNotContain != null) {
            assertFalse(sql.contains(mustNotContain),
                    "Did not expect [%s] in: %s".formatted(mustNotContain, sql));
        }
    }

    static Stream<Arguments> pagedQueryCases() {
        return Stream.of(
            Arguments.of("PostgreSQL LIMIT/OFFSET",
                ConnectionType.Postgres,  "LIMIT 50 OFFSET 100",  "FETCH NEXT"),
            Arguments.of("MySQL LIMIT/OFFSET",
                ConnectionType.MySql,     "LIMIT 50 OFFSET 100",  "FETCH NEXT"),
            Arguments.of("SQL Server OFFSET/FETCH",
                ConnectionType.SqlServer, "OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY", "LIMIT"),
            Arguments.of("Oracle OFFSET/FETCH",
                ConnectionType.Oracle,    "OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY", "LIMIT")
        );
    }

    @Test
    void buildPagedRootQuery_withWhereClause_appendsWhere() {
        XmlTableMapping m = xmlMapping("public", "employee", List.of(xmlCol("id")));
        String sql = builder.buildPagedRootQuery(m, ConnectionType.Postgres, "dept_id = 5", 0L, 100L);
        assertTrue(sql.contains("WHERE dept_id = 5"));
    }

    // ── Column select list ────────────────────────────────────────────────────

    @Test
    void buildRootQuery_nullColumns_selectsStar() {
        XmlTableMapping m = xmlMapping("public", "employee", null);
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("SELECT *"));
    }

    @Test
    void buildRootQuery_emptyColumns_selectsStar() {
        XmlTableMapping m = xmlMapping("public", "employee", List.of());
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("SELECT *"));
    }

    @Test
    void buildRootQuery_customColumnsOnly_selectsStar() {
        // CUSTOM columns have no source column — should fall back to *
        XmlTableMapping m = xmlMapping("public", "employee", List.of(xmlCustomCol()));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("SELECT *"));
    }

    @Test
    void buildRootQuery_mixedCustomAndRegularColumns_excludesCustom() {
        XmlColumnMapping regular = xmlCol("first_name");
        XmlColumnMapping custom  = xmlCustomCol();
        XmlTableMapping m = xmlMapping("public", "employee", List.of(regular, custom));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("\"first_name\""));
        assertFalse(sql.contains("*"));
    }

    @Test
    void buildRootQuery_columnWithBlankSourceColumn_excluded() {
        XmlColumnMapping blank = new XmlColumnMapping();
        blank.setSourceColumn("   ");
        blank.setMappingType("Element");
        XmlColumnMapping valid = xmlCol("id");
        XmlTableMapping m = xmlMapping("public", "employee", List.of(blank, valid));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("\"id\""));
        assertFalse(sql.contains("\"   \""));
    }

    // ── JSON select list ──────────────────────────────────────────────────────

    @Test
    void buildRootQuery_json_nullColumns_selectsStar() {
        JsonTableMapping m = jsonMapping("public", "employee", null);
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("SELECT *"));
    }

    @Test
    void buildRootQuery_json_customColumnsOnly_selectsStar() {
        JsonTableMapping m = jsonMapping("public", "employee", List.of(jsonCustomCol()));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("SELECT *"));
    }

    @Test
    void buildRootQuery_json_regularColumns_selectedExplicitly() {
        JsonTableMapping m = jsonMapping("public", "employee",
                List.of(jsonCol("id"), jsonCol("email")));
        String sql = builder.buildRootQuery(m, ConnectionType.Postgres, 10);
        assertTrue(sql.contains("\"id\""));
        assertTrue(sql.contains("\"email\""));
        assertFalse(sql.contains("*"));
    }

    // ── Identifier quoting ────────────────────────────────────────────────────

    @Test
    void buildChildQuery_columnNameWithDoubleQuote_isEscaped() {
        XmlTableMapping m = xmlMapping("public", "t", List.of(xmlCol("col\"name")));
        String sql = builder.buildChildQuery(m, "fk");
        assertTrue(sql.contains("\"col\"\"name\""));
    }
}
