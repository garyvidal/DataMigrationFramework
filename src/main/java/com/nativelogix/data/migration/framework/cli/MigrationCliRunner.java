package com.nativelogix.data.migration.framework.cli;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

/**
 * Activated when the application is started with --spring.profiles.active=cli.
 * Delegates all argument parsing and execution to {@link MigrationCliCommand} via Picocli.
 *
 * Usage:
 *   java -jar DataMigrationFramework.jar --spring.profiles.active=cli \
 *        --project "My Project" \
 *        --marklogic-connection "ML Prod" \
 *        [--source-connection "Oracle Dev"] \
 *        [--directory "/orders/"] \
 *        [--collection orders,archived]
 *
 *   # List available resources:
 *   java -jar DataMigrationFramework.jar --spring.profiles.active=cli --list-projects
 *   java -jar DataMigrationFramework.jar --spring.profiles.active=cli --list-connections
 *   java -jar DataMigrationFramework.jar --spring.profiles.active=cli --list-ml-connections
 */
@Slf4j
@Component
@Profile("cli")
@RequiredArgsConstructor
public class MigrationCliRunner implements CommandLineRunner, ExitCodeGenerator {

    private final MigrationCliCommand command;

    private int exitCode = 0;

    @Override
    public void run(String... args) {
        // Strip Spring Boot's own --spring.profiles.active= argument so Picocli doesn't choke on it
        String[] cliArgs = filterSpringArgs(args);
        exitCode = new CommandLine(command).execute(cliArgs);
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    private String[] filterSpringArgs(String[] args) {
        return java.util.Arrays.stream(args)
                .filter(a -> !a.startsWith("--spring.") && !a.startsWith("--server."))
                .toArray(String[]::new);
    }
}
