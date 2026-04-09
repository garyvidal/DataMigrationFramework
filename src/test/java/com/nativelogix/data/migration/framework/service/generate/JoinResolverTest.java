package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.JoinCondition;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.project.SyntheticJoin;
import com.nativelogix.data.migration.framework.model.relational.DbRelationship;
import com.nativelogix.data.migration.framework.model.relational.DbSchema;
import com.nativelogix.data.migration.framework.model.relational.DbTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JoinResolverTest {

    private final JoinResolver resolver = new JoinResolver();

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Build a minimal Project with one schema containing a parent and child table. */
    private Project projectWithFk(String schema,
                                   String parentTable, String fkFromCol,
                                   String childTable,  String fkToCol) {
        DbRelationship rel = new DbRelationship();
        rel.setFromColumn(fkFromCol);
        rel.setToTable(schema + "." + childTable);
        rel.setToColumn(fkToCol);

        DbTable parent = new DbTable();
        parent.setTableName(parentTable);
        parent.setRelationships(List.of(rel));

        DbTable child = new DbTable();
        child.setTableName(childTable);

        DbSchema dbSchema = new DbSchema();
        dbSchema.setName(schema);
        dbSchema.setTables(Map.of(parentTable, parent, childTable, child));

        Project project = new Project();
        project.setSchemas(Map.of(schema, dbSchema));
        return project;
    }

    /** Build a minimal SourceTableRef. */
    private JoinResolver.SourceTableRef ref(String schema, String table) {
        return new JoinResolver.SourceTableRef(schema, table, null, null);
    }

    // ── FK on parent table (forward) ──────────────────────────────────────────

    @Test
    void resolve_forwardForeignKey_returnsCorrectPath() {
        Project project = projectWithFk("public", "order", "customer_id", "customer", "id");

        JoinResolver.JoinPath path = resolver.resolve(
                ref("public", "order"),
                ref("public", "customer"),
                project);

        assertEquals("customer_id", path.parentColumn());
        assertEquals("id", path.childColumn());
    }

    // ── FK on child table (reverse) ───────────────────────────────────────────

    @Test
    void resolve_reverseForeignKey_returnsSwappedPath() {
        // FK lives on order_item pointing back to order
        DbRelationship rel = new DbRelationship();
        rel.setFromColumn("order_id");
        rel.setToTable("public.order");
        rel.setToColumn("id");

        DbTable orderItem = new DbTable();
        orderItem.setTableName("order_item");
        orderItem.setRelationships(List.of(rel));

        DbTable order = new DbTable();
        order.setTableName("order");

        DbSchema schema = new DbSchema();
        schema.setName("public");
        schema.setTables(Map.of("order", order, "order_item", orderItem));

        Project project = new Project();
        project.setSchemas(Map.of("public", schema));

        JoinResolver.JoinPath path = resolver.resolve(
                ref("public", "order"),
                ref("public", "order_item"),
                project);

        // parent=order, child=order_item; FK is on child → reversed
        assertEquals("id",       path.parentColumn());
        assertEquals("order_id", path.childColumn());
    }

    // ── Bare table name in toTable (legacy data) ──────────────────────────────

    @Test
    void resolve_bareTableNameInRelationship_stillMatches() {
        DbRelationship rel = new DbRelationship();
        rel.setFromColumn("dept_id");
        rel.setToTable("department");   // bare name, no schema prefix
        rel.setToColumn("id");

        DbTable employee = new DbTable();
        employee.setTableName("employee");
        employee.setRelationships(List.of(rel));

        DbTable department = new DbTable();
        department.setTableName("department");

        DbSchema schema = new DbSchema();
        schema.setName("hr");
        schema.setTables(Map.of("employee", employee, "department", department));

        Project project = new Project();
        project.setSchemas(Map.of("hr", schema));

        JoinResolver.JoinPath path = resolver.resolve(
                ref("hr", "employee"),
                ref("hr", "department"),
                project);

        assertEquals("dept_id", path.parentColumn());
        assertEquals("id", path.childColumn());
    }

    // ── Synthetic join (no FK) ────────────────────────────────────────────────

    @Test
    void resolve_syntheticJoinForward_returnsConditionColumns() {
        JoinCondition condition = new JoinCondition();
        condition.setSourceColumn("ext_id");
        condition.setTargetColumn("ref_id");

        SyntheticJoin sj = new SyntheticJoin();
        sj.setSourceSchema("dbo");
        sj.setSourceTable("order");
        sj.setTargetSchema("dbo");
        sj.setTargetTable("shipment");
        sj.setConditions(List.of(condition));

        Project project = new Project();
        project.setSyntheticJoins(List.of(sj));
        // no schemas / FK relationships

        JoinResolver.JoinPath path = resolver.resolve(
                ref("dbo", "order"),
                ref("dbo", "shipment"),
                project);

        assertEquals("ext_id", path.parentColumn());
        assertEquals("ref_id", path.childColumn());
    }

    @Test
    void resolve_syntheticJoinReverse_returnsSwappedColumns() {
        JoinCondition condition = new JoinCondition();
        condition.setSourceColumn("ext_id");
        condition.setTargetColumn("ref_id");

        SyntheticJoin sj = new SyntheticJoin();
        sj.setSourceSchema("dbo");
        sj.setSourceTable("shipment");  // stored as source
        sj.setTargetSchema("dbo");
        sj.setTargetTable("order");     // stored as target
        sj.setConditions(List.of(condition));

        Project project = new Project();
        project.setSyntheticJoins(List.of(sj));

        // Resolve parent=order, child=shipment — the reverse of how SJ is stored
        JoinResolver.JoinPath path = resolver.resolve(
                ref("dbo", "order"),
                ref("dbo", "shipment"),
                project);

        assertEquals("ref_id", path.parentColumn());
        assertEquals("ext_id", path.childColumn());
    }

    // ── No join found ─────────────────────────────────────────────────────────

    @Test
    void resolve_noJoinFound_throwsIllegalArgumentException() {
        Project project = new Project();
        // empty project — no schemas, no synthetic joins

        assertThrows(IllegalArgumentException.class, () ->
                resolver.resolve(ref("public", "foo"), ref("public", "bar"), project));
    }

    // ── Case insensitivity ────────────────────────────────────────────────────

    @Test
    void resolve_tableNameCaseInsensitive_matches() {
        Project project = projectWithFk("PUBLIC", "ORDER", "customer_id", "CUSTOMER", "id");

        // Resolve with lowercase refs — should still match
        JoinResolver.JoinPath path = resolver.resolve(
                ref("public", "order"),
                ref("public", "customer"),
                project);

        assertEquals("customer_id", path.parentColumn());
        assertEquals("id", path.childColumn());
    }

    // ── FK with matching joinColumn hint ─────────────────────────────────────

    @Test
    void resolve_multipleRelationships_joinColumnHintSelectsCorrectOne() {
        DbRelationship rel1 = new DbRelationship();
        rel1.setFromColumn("billing_id");
        rel1.setToTable("public.address");
        rel1.setToColumn("id");

        DbRelationship rel2 = new DbRelationship();
        rel2.setFromColumn("shipping_id");
        rel2.setToTable("public.address");
        rel2.setToColumn("id");

        DbTable order = new DbTable();
        order.setTableName("order");
        order.setRelationships(List.of(rel1, rel2));

        DbTable address = new DbTable();
        address.setTableName("address");

        DbSchema schema = new DbSchema();
        schema.setName("public");
        schema.setTables(Map.of("order", order, "address", address));

        Project project = new Project();
        project.setSchemas(Map.of("public", schema));

        // Child ref with joinColumn hint selects shipping_id FK
        JoinResolver.SourceTableRef childRef =
                new JoinResolver.SourceTableRef("public", "address", null, "shipping_id");

        JoinResolver.JoinPath path = resolver.resolve(ref("public", "order"), childRef, project);

        assertEquals("shipping_id", path.parentColumn());
        assertEquals("id", path.childColumn());
    }
}
