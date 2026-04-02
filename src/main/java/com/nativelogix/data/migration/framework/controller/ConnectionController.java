package com.nativelogix.data.migration.framework.controller;

import com.nativelogix.data.migration.framework.model.Connection;
import com.nativelogix.data.migration.framework.model.ConnectionTestResult;
import com.nativelogix.data.migration.framework.model.SavedConnection;
import com.nativelogix.data.migration.framework.model.requests.SaveConnectionRequest;
import com.nativelogix.data.migration.framework.service.JDBCConnectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/connections")
public class ConnectionController {

    @Autowired
    private JDBCConnectionService JDBCConnectionService;

    /** Test a connection using credentials supplied directly in the request body. */
    @PostMapping("/test")
    public ResponseEntity<ConnectionTestResult> testConnection(@RequestBody Connection connection) {
        return ResponseEntity.ok(JDBCConnectionService.testConnection(connection));
    }

    /** Test a saved connection using its stored (decrypted) credentials — no password needed from client. */
    @PostMapping("/{id}/test")
    public ResponseEntity<ConnectionTestResult> testConnectionById(@PathVariable String id) {
        return ResponseEntity.ok(JDBCConnectionService.testConnectionById(id));
    }

    @PostMapping
    public ResponseEntity<SavedConnection> saveConnection(@RequestBody SaveConnectionRequest request) {
        SavedConnection sc = new SavedConnection(
                request.getId(),
                request.getName(),
                request.getEnvironment(),
                request.getConnection()
        );
        return ResponseEntity.ok(masked(JDBCConnectionService.saveConnection(sc)));
    }

    @PutMapping("/{name}")
    public ResponseEntity<SavedConnection> updateConnection(
            @PathVariable String name,
            @RequestBody SaveConnectionRequest request) {
        SavedConnection sc = new SavedConnection(
                request.getId(),
                request.getName(),
                request.getEnvironment(),
                request.getConnection()
        );
        return ResponseEntity.ok(masked(JDBCConnectionService.updateConnection(name, sc)));
    }

    @GetMapping
    public ResponseEntity<List<SavedConnection>> getAllConnections() {
        return ResponseEntity.ok(JDBCConnectionService.getAllConnections().stream()
                .map(this::masked)
                .toList());
    }

    @GetMapping("/{name}")
    public ResponseEntity<SavedConnection> getConnection(@PathVariable String name) {
        return JDBCConnectionService.getConnection(name)
                .map(sc -> ResponseEntity.ok(masked(sc)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> deleteConnection(@PathVariable String name) {
        if (!JDBCConnectionService.connectionExists(name)) {
            return ResponseEntity.notFound().build();
        }
        JDBCConnectionService.deleteConnection(name);
        return ResponseEntity.noContent().build();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns a copy of the SavedConnection with the password field cleared so
     * that plaintext credentials are never sent to the frontend.
     */
    private SavedConnection masked(SavedConnection sc) {
        if (sc == null || sc.getConnection() == null) return sc;
        Connection c = sc.getConnection();
        Connection safe = new Connection();
        safe.setType(c.getType());
        safe.setUrl(c.getUrl());
        safe.setPort(c.getPort());
        safe.setDatabase(c.getDatabase());
        safe.setUserName(c.getUserName());
        safe.setPassword(null); // never return the password to the client
        safe.setEnterUriManually(c.getEnterUriManually());
        safe.setJdbcUri(c.getJdbcUri());
        safe.setAuthentication(c.getAuthentication());
        safe.setIdentifier(c.getIdentifier());
        safe.setPdbName(c.getPdbName());
        safe.setUseSSL(c.getUseSSL());
        safe.setSslMode(c.getSslMode());
        return new SavedConnection(sc.getId(), sc.getName(), sc.getEnvironment(), safe);
    }
}
