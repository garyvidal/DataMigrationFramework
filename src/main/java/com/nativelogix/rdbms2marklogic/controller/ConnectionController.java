package com.nativelogix.rdbms2marklogic.controller;

import com.nativelogix.rdbms2marklogic.model.Connection;
import com.nativelogix.rdbms2marklogic.model.ConnectionTestResult;
import com.nativelogix.rdbms2marklogic.model.SavedConnection;
import com.nativelogix.rdbms2marklogic.model.requests.SaveConnectionRequest;
import com.nativelogix.rdbms2marklogic.repository.FileSystemConnectionRepository;
import com.nativelogix.rdbms2marklogic.service.ConnectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/v1/connections")
@CrossOrigin(origins = "http://localhost:5173", allowedHeaders = "*",
        methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.OPTIONS})
public class ConnectionController {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private FileSystemConnectionRepository connectionRepository;

    @PostMapping("/test")
    public ResponseEntity<ConnectionTestResult> testConnection(@RequestBody Connection connection) {
        return ResponseEntity.ok(connectionService.testConnection(connection));
    }

    @PostMapping
    public ResponseEntity<SavedConnection> saveConnection(@RequestBody SaveConnectionRequest request) {
        Connection saved = connectionService.saveConnection(request.getName(), request.getConnection());
        return ResponseEntity.ok(new SavedConnection(request.getName(), saved));
    }

    @GetMapping
    public ResponseEntity<List<SavedConnection>> getAllConnections() {
        List<SavedConnection> connections = connectionRepository.findAllWithNames().entrySet()
                .stream()
                .map(e -> new SavedConnection(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(connections);
    }

    @GetMapping("/{name}")
    public ResponseEntity<SavedConnection> getConnection(@PathVariable String name) {
        return connectionService.getConnection(name)
                .map(conn -> ResponseEntity.ok(new SavedConnection(name, conn)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> deleteConnection(@PathVariable String name) {
        if (!connectionService.connectionExists(name)) {
            return ResponseEntity.notFound().build();
        }
        connectionService.deleteConnection(name);
        return ResponseEntity.noContent().build();
    }
}
