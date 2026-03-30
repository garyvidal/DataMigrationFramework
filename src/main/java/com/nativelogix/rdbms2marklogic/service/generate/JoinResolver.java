package com.nativelogix.rdbms2marklogic.service.generate;

import com.nativelogix.rdbms2marklogic.model.project.JoinCondition;
import com.nativelogix.rdbms2marklogic.model.project.JsonTableMapping;
import com.nativelogix.rdbms2marklogic.model.project.Project;
import com.nativelogix.rdbms2marklogic.model.project.SyntheticJoin;
import com.nativelogix.rdbms2marklogic.model.project.XmlTableMapping;
import com.nativelogix.rdbms2marklogic.model.relational.DbRelationship;
import com.nativelogix.rdbms2marklogic.model.relational.DbSchema;
import com.nativelogix.rdbms2marklogic.model.relational.DbTable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Resolves the join path between two table mappings using the project's
 * FK relationships first, then falling back to user-defined synthetic joins.
 */
@Component
public class  JoinResolver {

    /**
     * Result of a join resolution: which column on the parent side and which column
     * on the child side link the two tables.
     */
    public record JoinPath(String parentColumn, String childColumn) {}

    /**
     * A format-agnostic reference to a mapped table, used so that both XML and JSON
     * generation can share the same join resolution logic.
     */
    public record SourceTableRef(String sourceSchema, String sourceTable, String id, String joinColumn) {
        public static SourceTableRef of(XmlTableMapping m) {
            return new SourceTableRef(m.getSourceSchema(), m.getSourceTable(), m.getId(), m.getJoinColumn());
        }
        public static SourceTableRef of(JsonTableMapping m) {
            return new SourceTableRef(m.getSourceSchema(), m.getSourceTable(), m.getId(), m.getJoinColumn());
        }
    }

    // ── XML convenience overload (unchanged API) ──────────────────────────────

    /**
     * Resolve how {@code parentMapping}'s table connects to {@code childMapping}'s table.
     *
     * @throws IllegalArgumentException if no join path can be found
     */
    public JoinPath resolve(XmlTableMapping parentMapping, XmlTableMapping childMapping, Project project) {
        return resolve(SourceTableRef.of(parentMapping), SourceTableRef.of(childMapping), project);
    }

    // ── JSON overload ─────────────────────────────────────────────────────────

    public JoinPath resolve(JsonTableMapping parentMapping, JsonTableMapping childMapping, Project project) {
        return resolve(SourceTableRef.of(parentMapping), SourceTableRef.of(childMapping), project);
    }

    // ── Core resolution (format-agnostic) ────────────────────────────────────

    public JoinPath resolve(SourceTableRef parent, SourceTableRef child, Project project) {
        // 1. Try FK relationships stored on the parent table
        JoinPath fkPath = resolveViaForeignKey(parent, child, project);
        if (fkPath != null) return fkPath;

        // 2. Try in reverse: FK on child table pointing back to parent
        JoinPath reverseFkPath = resolveViaForeignKeyReverse(parent, child, project);
        if (reverseFkPath != null) return reverseFkPath;

        // 3. Try user-defined synthetic joins
        JoinPath syntheticPath = resolveViaSyntheticJoin(parent, child, project);
        if (syntheticPath != null) return syntheticPath;

        throw new IllegalArgumentException(
                "No join path found between '%s.%s' and '%s.%s'. Add a synthetic join or ensure a foreign key exists."
                        .formatted(parent.sourceSchema(), parent.sourceTable(),
                                   child.sourceSchema(), child.sourceTable()));
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private JoinPath resolveViaForeignKey(SourceTableRef parent, SourceTableRef child, Project project) {
        DbTable parentTable = getDbTable(project, parent.sourceSchema(), parent.sourceTable());
        if (parentTable == null || parentTable.getRelationships() == null) return null;

        String childFullName = child.sourceSchema() + "." + child.sourceTable();
        for (DbRelationship rel : parentTable.getRelationships()) {
            if (toTableMatches(rel.getToTable(), child.sourceTable(), childFullName)) {
                // If a specific FK column was requested, skip non-matching ones
                if (child.joinColumn() != null && !child.joinColumn().equalsIgnoreCase(rel.getFromColumn())) continue;
                return new JoinPath(rel.getFromColumn(), rel.getToColumn());
            }
        }
        return null;
    }

    private JoinPath resolveViaForeignKeyReverse(SourceTableRef parent, SourceTableRef child, Project project) {
        DbTable childTable = getDbTable(project, child.sourceSchema(), child.sourceTable());
        if (childTable == null || childTable.getRelationships() == null) return null;

        String parentFullName = parent.sourceSchema() + "." + parent.sourceTable();
        for (DbRelationship rel : childTable.getRelationships()) {
            if (toTableMatches(rel.getToTable(), parent.sourceTable(), parentFullName)) {
                // If a specific FK column was requested, skip non-matching ones
                if (child.joinColumn() != null && !child.joinColumn().equalsIgnoreCase(rel.getFromColumn())) continue;
                return new JoinPath(rel.getToColumn(), rel.getFromColumn());
            }
        }
        return null;
    }

    /**
     * Matches a stored toTable value (which may be a bare name or a full dotted name)
     * against a target table. Handles old data (bare names) and new data (full names).
     */
    private boolean toTableMatches(String toTable, String bareTableName, String fullTableName) {
        if (toTable == null) return false;
        return toTable.equalsIgnoreCase(bareTableName)
            || toTable.equalsIgnoreCase(fullTableName)
            || toTable.toLowerCase().endsWith("." + bareTableName.toLowerCase());
    }

    private JoinPath resolveViaSyntheticJoin(SourceTableRef parent, SourceTableRef child, Project project) {
        if (project.getSyntheticJoins() == null) return null;

        for (SyntheticJoin sj : project.getSyntheticJoins()) {
            List<JoinCondition> conditions = sj.getConditions();
            if (conditions == null || conditions.isEmpty()) continue;

            // Forward: source=parent, target=child
            if (matches(sj.getSourceSchema(), sj.getSourceTable(), parent) &&
                matches(sj.getTargetSchema(), sj.getTargetTable(), child)) {
                JoinCondition first = conditions.get(0);
                return new JoinPath(first.getSourceColumn(), first.getTargetColumn());
            }

            // Reverse: source=child, target=parent
            if (matches(sj.getSourceSchema(), sj.getSourceTable(), child) &&
                matches(sj.getTargetSchema(), sj.getTargetTable(), parent)) {
                JoinCondition first = conditions.get(0);
                return new JoinPath(first.getTargetColumn(), first.getSourceColumn());
            }
        }
        return null;
    }

    private boolean matches(String schema, String table, SourceTableRef ref) {
        return table.equalsIgnoreCase(ref.sourceTable()) &&
               (schema == null || schema.equalsIgnoreCase(ref.sourceSchema()));
    }

    private DbTable getDbTable(Project project, String schemaName, String tableName) {
        if (project.getSchemas() == null) return null;
        for (Map.Entry<String, DbSchema> schemaEntry : project.getSchemas().entrySet()) {
            DbSchema schema = schemaEntry.getValue();
            if (schema == null || schema.getTables() == null) continue;
            if (!schemaEntry.getKey().equalsIgnoreCase(schemaName) &&
                (schema.getName() == null || !schema.getName().equalsIgnoreCase(schemaName))) continue;
            for (Map.Entry<String, DbTable> tableEntry : schema.getTables().entrySet()) {
                if (tableEntry.getKey().equalsIgnoreCase(tableName)) {
                    return tableEntry.getValue();
                }
            }
        }
        return null;
    }
}
