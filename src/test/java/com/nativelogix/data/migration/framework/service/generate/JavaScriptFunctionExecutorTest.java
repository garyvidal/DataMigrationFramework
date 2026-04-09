package com.nativelogix.data.migration.framework.service.generate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JavaScriptFunctionExecutorTest {

    private final JavaScriptFunctionExecutor executor = new JavaScriptFunctionExecutor();

    // ── Blank / null function body ────────────────────────────────────────────

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void evaluate_blankFunctionBody_returnsNull(String body) {
        assertNull(executor.evaluate(body, Map.of("col", "value")));
    }

    // ── Simple expressions ────────────────────────────────────────────────────

    @Test
    void evaluate_literalExpression_returnsValue() {
        assertEquals("hello", executor.evaluate("'hello'", Map.of()));
    }

    @Test
    void evaluate_columnConcatenation_returnsJoinedValue() {
        Map<String, Object> row = Map.of("first_name", "Gary", "last_name", "Vidal");
        assertEquals("Gary Vidal", executor.evaluate("row.first_name + ' ' + row.last_name", row));
    }

    @Test
    void evaluate_withReturnStatement_returnsValue() {
        Map<String, Object> row = Map.of("price", 42);
        assertEquals("42", executor.evaluate("return row.price.toString();", row));
    }

    @Test
    void evaluate_numericExpression_returnsStringRepresentation() {
        Map<String, Object> row = Map.of("qty", 3, "unit_price", 5);
        String result = executor.evaluate("row.qty * row.unit_price", row);
        assertNotNull(result);
        assertEquals("15", result);
    }

    // ── Null / missing columns ────────────────────────────────────────────────

    @Test
    void evaluate_nullRowMap_doesNotThrow() {
        assertDoesNotThrow(() -> executor.evaluate("'constant'", null));
    }

    @Test
    void evaluate_missingColumn_returnsUndefinedAsNull() {
        // Accessing a property that doesn't exist in the row returns JS undefined → null
        assertNull(executor.evaluate("row.nonexistent_column", Map.of()));
    }

    @Test
    void evaluate_columnValueIsNull_doesNotThrow() {
        Map<String, Object> row = new java.util.HashMap<>();
        row.put("name", null);
        assertDoesNotThrow(() -> executor.evaluate("row.name", row));
    }

    // ── JavaScript undefined / null return ───────────────────────────────────

    @Test
    void evaluate_functionReturnsUndefined_returnsNull() {
        // A function with no return value yields JS undefined
        assertNull(executor.evaluate("var x = 1;", Map.of()));
    }

    @Test
    void evaluate_explicitReturnNull_returnsNull() {
        assertNull(executor.evaluate("return null;", Map.of()));
    }

    // ── Error handling ────────────────────────────────────────────────────────

    @Test
    void evaluate_syntaxError_returnsNullWithoutThrowing() {
        assertNull(executor.evaluate("{{{{ invalid js", Map.of()));
    }

    @Test
    void evaluate_runtimeError_returnsNullWithoutThrowing() {
        // Calling a method that doesn't exist will throw at runtime
        assertNull(executor.evaluate("row.noSuchMethod()", Map.of()));
    }

    // ── Script caching ────────────────────────────────────────────────────────

    @Test
    void evaluate_sameBodyCalledRepeatedly_returnsSameResult() {
        String body = "row.val + '!'";
        Map<String, Object> row = Map.of("val", "test");
        // First call compiles and caches; subsequent calls use cache
        assertEquals("test!", executor.evaluate(body, row));
        assertEquals("test!", executor.evaluate(body, row));
        assertEquals("test!", executor.evaluate(body, row));
    }

    @Test
    void evaluate_rowBindingsDoNotLeakBetweenCalls() {
        String body = "row.secret || 'none'";
        Map<String, Object> rowWithSecret = Map.of("secret", "sensitive");
        Map<String, Object> rowWithout    = Map.of();

        assertEquals("sensitive", executor.evaluate(body, rowWithSecret));
        // Second call must NOT see the first call's row binding
        assertEquals("none", executor.evaluate(body, rowWithout));
    }

    // ── Type coercion ─────────────────────────────────────────────────────────

    @Test
    void evaluate_booleanValue_returnsStringRepresentation() {
        Map<String, Object> row = Map.of("active", true);
        assertEquals("true", executor.evaluate("row.active", row));
    }

    @Test
    void evaluate_conditionalExpression_returnsCorrectBranch() {
        Map<String, Object> row = Map.of("status", "A");
        assertEquals("Active",
                executor.evaluate("row.status === 'A' ? 'Active' : 'Inactive'", row));
    }
}
