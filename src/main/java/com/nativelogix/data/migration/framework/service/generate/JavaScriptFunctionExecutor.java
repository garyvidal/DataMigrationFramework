package com.nativelogix.data.migration.framework.service.generate;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes user-defined JavaScript functions against a database row using Mozilla Rhino.
 *
 * <h3>Function contract</h3>
 * The {@code customFunction} string stored in the mapping is a <em>function body</em>
 * (not a declaration). It receives the current row as {@code row} — a plain JS object
 * whose properties mirror the database column names — and is expected to {@code return}
 * a value:
 * <pre>
 *   return row.first_name + ' ' + row.last_name;
 * </pre>
 *
 * <h3>Performance</h3>
 * <ul>
 *   <li><b>Script cache</b> — compiled {@link Script} objects are cached by function text
 *       and reused across all rows and threads.</li>
 *   <li><b>ThreadLocal scope</b> — {@code cx.initStandardObjects()} is expensive (allocates
 *       the full JS standard library). Each worker thread initialises its scope once and
 *       reuses it for every subsequent evaluation, eliminating the per-call overhead.</li>
 *   <li><b>Reused Context</b> — a single Rhino {@link Context} is entered once per thread
 *       and exited when the thread terminates (via {@link ThreadLocal} cleanup), avoiding
 *       the enter/exit handshake on every call.</li>
 * </ul>
 */
@Component
public class JavaScriptFunctionExecutor {

    private static final Logger log = LoggerFactory.getLogger(JavaScriptFunctionExecutor.class);

    /** Compiled wrapper scripts keyed by raw function body text — shared across threads. */
    private final ConcurrentHashMap<String, Script> scriptCache = new ConcurrentHashMap<>();

    /**
     * Per-thread Rhino context. Entered once when first used; held open for the lifetime
     * of the thread so we avoid the per-call enter/exit overhead.
     */
    private static final ThreadLocal<Context> THREAD_CONTEXT = ThreadLocal.withInitial(() -> {
        Context cx = Context.enter();
        cx.setOptimizationLevel(-1);
        cx.setLanguageVersion(Context.VERSION_ES6);
        return cx;
    });

    /**
     * Per-thread standard scope. Initialised once per thread from the thread's Context;
     * reused for every evaluation so {@code initStandardObjects()} runs only once per thread.
     */
    private static final ThreadLocal<Scriptable> THREAD_SCOPE = ThreadLocal.withInitial(() ->
            THREAD_CONTEXT.get().initStandardObjects());

    /**
     * Evaluates {@code functionBody} against {@code row} and returns the result as a
     * {@link String}, or {@code null} if the function body is blank, throws, or returns
     * {@code undefined}/{@code null}.
     *
     * @param functionBody JavaScript function body (no wrapping {@code function} keyword)
     * @param row          JDBC column name → value for the current row
     * @return string representation of the return value, or {@code null}
     */
    public String evaluate(String functionBody, Map<String, Object> row) {
        if (functionBody == null || functionBody.isBlank()) return null;

        Context cx    = THREAD_CONTEXT.get();
        Scriptable sharedScope = THREAD_SCOPE.get();

        try {
            // Create a fresh child scope per call so row bindings don't leak between calls,
            // but standard objects are inherited from the shared parent — no re-initialisation.
            Scriptable callScope = cx.newObject(sharedScope);
            callScope.setPrototype(sharedScope);
            callScope.setParentScope(null);

            NativeObject rowObj = buildRowObject(callScope, row, cx);
            ScriptableObject.putProperty(callScope, "row", rowObj);

            Script script = scriptCache.computeIfAbsent(functionBody, body -> {
                String wrapped = hasReturnStatement(body)
                        ? "(function(row){ " + body + " })(row);"
                        : "(function(row){ return (" + body + ") })(row);";
                return cx.compileString(wrapped, "<custom-function>", 1, null);
            });

            Object result = script.exec(cx, callScope);
            if (result == null || result instanceof Undefined) return null;
            return Context.toString(result);

        } catch (Exception e) {
            log.warn("Custom JS function evaluation failed: {} — body: [{}]",
                    e.getMessage(), abbreviate(functionBody, 120));
            return null;
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private NativeObject buildRowObject(Scriptable scope, Map<String, Object> row, Context cx) {
        NativeObject obj = (NativeObject) cx.newObject(scope);
        if (row == null) return obj;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Object jsValue = entry.getValue() == null
                    ? null
                    : Context.javaToJS(entry.getValue(), scope);
            ScriptableObject.putProperty(obj, entry.getKey(), jsValue);
        }
        return obj;
    }

    private boolean hasReturnStatement(String body) {
        return body.matches("(?s).*\\breturn\\b.*");
    }

    private String abbreviate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
