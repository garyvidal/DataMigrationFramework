package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.model.ConnectionTestResult;
import com.nativelogix.data.migration.framework.model.MarkLogicConnection;
import com.nativelogix.data.migration.framework.model.SavedMarkLogicConnection;
import com.nativelogix.data.migration.framework.model.requests.SaveMarkLogicConnectionRequest;
import com.nativelogix.data.migration.framework.service.MarkLogicConnectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/marklogic/connections")
@RequiredArgsConstructor
public class MarkLogicConnectionController {

    private final MarkLogicConnectionService service;

    /** Test a connection using credentials supplied directly in the request body. */
    @PostMapping("/test")
    public ResponseEntity<ConnectionTestResult> testConnection(@RequestBody MarkLogicConnection connection) {
        return ResponseEntity.ok(service.testConnection(connection));
    }

    /** Test a saved connection using its stored (decrypted) credentials — no password needed from client. */
    @PostMapping("/{id}/test")
    public ResponseEntity<ConnectionTestResult> testConnectionById(@PathVariable String id) {
        return ResponseEntity.ok(service.testConnectionById(id));
    }

    @PostMapping
    public ResponseEntity<SavedMarkLogicConnection> saveConnection(
            @RequestBody SaveMarkLogicConnectionRequest request) {
        SavedMarkLogicConnection sc = new SavedMarkLogicConnection(
                request.getId(), request.getName(), request.getConnection());
        return ResponseEntity.ok(masked(service.saveConnection(sc)));
    }

    @PutMapping("/{name}")
    public ResponseEntity<SavedMarkLogicConnection> updateConnection(
            @PathVariable String name,
            @RequestBody SaveMarkLogicConnectionRequest request) {
        SavedMarkLogicConnection sc = new SavedMarkLogicConnection(
                request.getId(), request.getName(), request.getConnection());
        return ResponseEntity.ok(masked(service.updateConnection(name, sc)));
    }

    @GetMapping
    public ResponseEntity<List<SavedMarkLogicConnection>> getAllConnections() {
        return ResponseEntity.ok(service.getAllConnections().stream()
                .map(this::masked)
                .toList());
    }

    @GetMapping("/{name}")
    public ResponseEntity<SavedMarkLogicConnection> getConnection(@PathVariable String name) {
        return service.getConnection(name)
                .map(sc -> ResponseEntity.ok(masked(sc)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> deleteConnection(@PathVariable String name) {
        if (!service.connectionExists(name)) {
            return ResponseEntity.notFound().build();
        }
        service.deleteConnection(name);
        return ResponseEntity.noContent().build();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns a copy of the SavedMarkLogicConnection with the password cleared so
     * that credentials are never sent to the frontend.
     */
    private SavedMarkLogicConnection masked(SavedMarkLogicConnection sc) {
        if (sc == null || sc.getConnection() == null) return sc;
        MarkLogicConnection c = sc.getConnection();
        MarkLogicConnection safe = new MarkLogicConnection();
        safe.setHost(c.getHost());
        safe.setPort(c.getPort());
        safe.setDatabase(c.getDatabase());
        safe.setUsername(c.getUsername());
        safe.setPassword(null); // never return the password to the client
        safe.setAuthType(c.getAuthType());
        safe.setUseSSL(c.getUseSSL());
        return new SavedMarkLogicConnection(sc.getId(), sc.getName(), safe);
    }
}
