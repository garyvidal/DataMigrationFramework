package com.nativelogix.data.migration.framework.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nativelogix.data.migration.framework.model.ImportResult;
import com.nativelogix.data.migration.framework.model.MigrationPackage;
import com.nativelogix.data.migration.framework.model.migration.DeploymentJob;
import com.nativelogix.data.migration.framework.model.migration.DeploymentJobStatus;
import com.nativelogix.data.migration.framework.model.migration.MigrationProgress;
import com.nativelogix.data.migration.framework.model.migration.MigrationRequest;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.repository.ConnectionRepository;
import com.nativelogix.data.migration.framework.repository.MarkLogicConnectionRepository;
import com.nativelogix.data.migration.framework.repository.ProjectRepository;
import com.nativelogix.data.migration.framework.service.PackageService;
import com.nativelogix.data.migration.framework.service.migration.MigrationJobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import picocli.CommandLine.*;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

@Slf4j
@Component
@Command(
    name = "migrate",
    description = "Run a DataMigrationFramework migration from the command line.",
    mixinStandardHelpOptions = true,
    sortOptions = false
)
@RequiredArgsConstructor
public class MigrationCliCommand implements Callable<Integer> {

    // ── Package mode ──────────────────────────────────────────────────────────

    @Option(
        names = {"--package"},
        description = "Path to a migration package file (.json). " +
                      "Loads project and connection config from the file. " +
                      "When provided, --project and --marklogic-connection are not required."
    )
    private File packageFile;

    @Option(
        names = {"--source-password"},
        description = "Password for the source database connection (used with --package). " +
                      "Accepts plaintext or a pre-encrypted value (prefix ENC:). " +
                      "Note: ENC: values are tied to the local encryption key and are not portable across machines.",
        interactive = false
    )
    private String sourcePassword;

    @Option(
        names = {"--marklogic-password"},
        description = "Password for the MarkLogic connection (used with --package). " +
                      "Accepts plaintext or a pre-encrypted value (prefix ENC:). " +
                      "Note: ENC: values are tied to the local encryption key and are not portable across machines.",
        interactive = false
    )
    private String marklogicPassword;

    // ── Required (unless --package is used) ───────────────────────────────────

    @Option(
        names = {"-p", "--project"},
        description = "Project name or ID to migrate."
    )
    private String projectNameOrId;

    @Option(
        names = {"-m", "--marklogic-connection"},
        description = "MarkLogic connection name or ID to write documents to."
    )
    private String mlConnectionNameOrId;

    // ── Optional ──────────────────────────────────────────────────────────────

    @Option(
        names = {"-s", "--source-connection"},
        description = "Source database connection name or ID. Defaults to the connection stored on the project."
    )
    private String sourceConnectionNameOrId;

    @Option(
        names = {"-d", "--directory"},
        description = "MarkLogic document directory path (e.g. /data/orders/). " +
                      "Supports {rootElement} and {index} placeholders.",
        defaultValue = "/"
    )
    private String directoryPath;

    @Option(
        names = {"-c", "--collection"},
        description = "MarkLogic collection(s) to assign to written documents. Repeatable.",
        split = ","
    )
    private List<String> collections;

    @Option(
        names = {"--poll-interval"},
        description = "Progress poll interval in milliseconds (default: 1000).",
        defaultValue = "1000"
    )
    private long pollIntervalMs;

    @Option(
        names = {"--list-projects"},
        description = "List all available projects and exit."
    )
    private boolean listProjects;

    @Option(
        names = {"--list-connections"},
        description = "List all available source DB connections and exit."
    )
    private boolean listConnections;

    @Option(
        names = {"--list-ml-connections"},
        description = "List all available MarkLogic connections and exit."
    )
    private boolean listMlConnections;

    // ── Dependencies ──────────────────────────────────────────────────────────

    private final MigrationJobService migrationJobService;
    private final ProjectRepository projectRepository;
    private final ConnectionRepository connectionRepository;
    private final MarkLogicConnectionRepository markLogicConnectionRepository;
    private final PackageService packageService;

    // ── Entry point ───────────────────────────────────────────────────────────

    @Override
    public Integer call() throws Exception {
        if (listProjects) {
            printProjects();
            return 0;
        }
        if (listConnections) {
            printConnections();
            return 0;
        }
        if (listMlConnections) {
            printMlConnections();
            return 0;
        }

        // ── Package mode: load project + connections from file ─────────────────
        if (packageFile != null) {
            return runWithPackage();
        }

        // ── Normal mode: require --project and --marklogic-connection ──────────
        if (projectNameOrId == null || projectNameOrId.isBlank()) {
            print(Ansi.RED + "Missing required option: --project (or use --package <file>)" + Ansi.RESET);
            return 1;
        }
        if (mlConnectionNameOrId == null || mlConnectionNameOrId.isBlank()) {
            print(Ansi.RED + "Missing required option: --marklogic-connection (or use --package <file>)" + Ansi.RESET);
            return 1;
        }

        return runNormal();
    }

    // ── Package-mode migration ────────────────────────────────────────────────

    private int runWithPackage() throws Exception {
        if (!packageFile.exists()) {
            print(Ansi.RED + "Package file not found: " + packageFile.getAbsolutePath() + Ansi.RESET);
            return 1;
        }

        print(Ansi.CYAN + "Loading package: " + Ansi.RESET + packageFile.getAbsolutePath());

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        MigrationPackage pkg = mapper.readValue(packageFile, MigrationPackage.class);

        // Import project and connections (skips anything already registered by ID)
        ImportResult imported = packageService.importPackage(pkg, sourcePassword, marklogicPassword);

        if (imported.isProjectCreated()) {
            print(Ansi.GREEN + "  ✓ Project imported: " + Ansi.RESET + imported.getProjectName());
        } else {
            print(Ansi.CYAN + "  · Project already registered: " + Ansi.RESET + imported.getProjectName());
        }
        if (imported.getSourceConnectionId() != null) {
            if (imported.isSourceConnectionCreated()) {
                print(Ansi.GREEN + "  ✓ Source connection imported: " + Ansi.RESET + imported.getSourceConnectionName());
            } else {
                print(Ansi.CYAN + "  · Source connection already registered: " + Ansi.RESET + imported.getSourceConnectionName());
            }
        }
        if (imported.getMarklogicConnectionId() != null) {
            if (imported.isMarklogicConnectionCreated()) {
                print(Ansi.GREEN + "  ✓ MarkLogic connection imported: " + Ansi.RESET + imported.getMarklogicConnectionName());
            } else {
                print(Ansi.CYAN + "  · MarkLogic connection already registered: " + Ansi.RESET + imported.getMarklogicConnectionName());
            }
        }
        imported.getWarnings().forEach(w -> print(Ansi.YELLOW + "  ⚠ " + w + Ansi.RESET));
        print("");

        // Resolve IDs — CLI flags override the package values
        String projectId = imported.getProjectId();
        if (projectNameOrId != null && !projectNameOrId.isBlank()) {
            projectId = resolveProject(projectNameOrId).getId();
        }
        if (projectId == null) {
            print(Ansi.RED + "Package does not contain a project." + Ansi.RESET);
            return 1;
        }

        String mlConnectionId = imported.getMarklogicConnectionId();
        if (mlConnectionNameOrId != null && !mlConnectionNameOrId.isBlank()) {
            mlConnectionId = resolveMlConnectionId(mlConnectionNameOrId);
        }
        if (mlConnectionId == null) {
            print(Ansi.RED + "No MarkLogic connection in package. Provide one with --marklogic-connection." + Ansi.RESET);
            return 1;
        }

        String sourceConnectionId = imported.getSourceConnectionId();
        if (sourceConnectionNameOrId != null && !sourceConnectionNameOrId.isBlank()) {
            sourceConnectionId = resolveSourceConnectionId(sourceConnectionNameOrId);
        }

        return runMigration(projectId, sourceConnectionId, mlConnectionId);
    }

    // ── Normal migration ──────────────────────────────────────────────────────

    private int runNormal() throws Exception {
        Project project = resolveProject(projectNameOrId);
        String sourceConnectionId = resolveSourceConnectionId(sourceConnectionNameOrId);
        String mlConnectionId = resolveMlConnectionId(mlConnectionNameOrId);

        print(Ansi.CYAN + "Project:              " + Ansi.RESET + project.getName());
        print(Ansi.CYAN + "Source connection:    " + Ansi.RESET + (sourceConnectionId != null ? sourceConnectionId : "(from project)"));
        print(Ansi.CYAN + "MarkLogic connection: " + Ansi.RESET + mlConnectionId);
        printDirectoryAndCollections();
        print("");

        return runMigration(project.getId(), sourceConnectionId, mlConnectionId);
    }

    // ── Shared migration execution ────────────────────────────────────────────

    private int runMigration(String projectId, String sourceConnectionId, String mlConnectionId) throws InterruptedException {
        // Show summary if we haven't already (package mode skips the top block)
        if (packageFile != null) {
            print(Ansi.CYAN + "Project ID:           " + Ansi.RESET + projectId);
            print(Ansi.CYAN + "Source connection:    " + Ansi.RESET + (sourceConnectionId != null ? sourceConnectionId : "(from project)"));
            print(Ansi.CYAN + "MarkLogic connection: " + Ansi.RESET + mlConnectionId);
            printDirectoryAndCollections();
            print("");
        }

        MigrationRequest req = new MigrationRequest();
        req.setProjectId(projectId);
        req.setSourceConnectionId(sourceConnectionId);
        req.setMarklogicConnectionId(mlConnectionId);
        req.setDirectoryPath(directoryPath);
        req.setCollections(collections);

        print(Ansi.YELLOW + "Starting migration..." + Ansi.RESET);
        DeploymentJob job = migrationJobService.startJob(req);
        print(Ansi.YELLOW + "Job ID: " + Ansi.RESET + job.getId());
        print("");

        return pollUntilDone(job.getId());
    }

    private void printDirectoryAndCollections() {
        print(Ansi.CYAN + "Directory:            " + Ansi.RESET + directoryPath);
        if (collections != null && !collections.isEmpty()) {
            print(Ansi.CYAN + "Collections:          " + Ansi.RESET + String.join(", ", collections));
        }
    }

    // ── Progress rendering ────────────────────────────────────────────────────

    private int pollUntilDone(String jobId) throws InterruptedException {
        final int barWidth = 40;
        long lastProcessed = 0;

        while (true) {
            Optional<MigrationProgress> opt = migrationJobService.getProgress(jobId);
            if (opt.isEmpty()) {
                print(Ansi.RED + "Job not found: " + jobId + Ansi.RESET);
                return 1;
            }

            MigrationProgress progress = opt.get();
            DeploymentJobStatus status = progress.getStatus();

            if (status == DeploymentJobStatus.PENDING) {
                printInPlace(Ansi.YELLOW + "Waiting for job to start..." + Ansi.RESET);
                Thread.sleep(pollIntervalMs);
                continue;
            }

            long total = progress.getTotalRecords();
            long processed = progress.getProcessedRecords();
            long elapsed = progress.getElapsedSeconds();

            long delta = processed - lastProcessed;
            double docsPerSec = pollIntervalMs > 0 ? (delta * 1000.0 / pollIntervalMs) : 0;
            lastProcessed = processed;

            String bar = renderBar(processed, total, barWidth);
            String pct = total > 0 ? String.format("%3.0f%%", (processed * 100.0 / total)) : "  ?%";
            String rate = String.format("%.0f docs/s", docsPerSec);
            String time = formatElapsed(elapsed);

            String line = String.format("%s%s%s %s  %s / %s  %s  [%s]",
                    Ansi.GREEN, bar, Ansi.RESET,
                    pct,
                    processed, total > 0 ? total : "?",
                    rate, time);

            if (status == DeploymentJobStatus.COMPLETED) {
                clearLine();
                print(Ansi.GREEN + "✓ Migration completed successfully!" + Ansi.RESET);
                print(String.format("  %s documents written in %s", processed, formatElapsed(elapsed)));
                return 0;
            }

            if (status == DeploymentJobStatus.FAILED) {
                clearLine();
                print(Ansi.RED + "✗ Migration failed." + Ansi.RESET);
                if (progress.getErrorMessage() != null) {
                    print(Ansi.RED + "  Error: " + progress.getErrorMessage() + Ansi.RESET);
                }
                return 1;
            }

            if (status == DeploymentJobStatus.CANCELLED) {
                clearLine();
                print(Ansi.YELLOW + "Migration was cancelled." + Ansi.RESET);
                return 1;
            }

            printInPlace(line);
            Thread.sleep(pollIntervalMs);
        }
    }

    private String renderBar(long processed, long total, int width) {
        if (total <= 0) {
            long tick = (System.currentTimeMillis() / 200) % width;
            char[] bar = new char[width];
            for (int i = 0; i < width; i++) bar[i] = i == tick ? '=' : '-';
            return "[" + new String(bar) + "]";
        }
        int filled = (int) Math.min(width, processed * width / total);
        return "[" + "=".repeat(filled) + (filled < width ? ">" : "") + " ".repeat(Math.max(0, width - filled - 1)) + "]";
    }

    // ── List helpers ──────────────────────────────────────────────────────────

    private void printProjects() {
        List<Project> projects = projectRepository.findAll();
        if (projects.isEmpty()) {
            print("No projects found.");
            return;
        }
        print(Ansi.CYAN + String.format("%-36s  %-30s  %s", "ID", "Name", "Connection") + Ansi.RESET);
        print("-".repeat(85));
        projects.forEach(p -> print(String.format("%-36s  %-30s  %s",
                p.getId(), p.getName(), p.getConnectionName() != null ? p.getConnectionName() : "-")));
    }

    private void printConnections() {
        connectionRepository.findAll().forEach(sc ->
                print(String.format("%-36s  %-30s  %s", sc.getId(), sc.getName(),
                        sc.getConnection() != null ? sc.getConnection().getType() + " " +
                        sc.getConnection().getUrl() + ":" + sc.getConnection().getPort() : "-")));
    }

    private void printMlConnections() {
        markLogicConnectionRepository.findAll().forEach(sc ->
                print(String.format("%-36s  %-30s  %s:%s", sc.getId(), sc.getName(),
                        sc.getConnection() != null ? sc.getConnection().getHost() : "-",
                        sc.getConnection() != null ? sc.getConnection().getPort() : "-")));
    }

    // ── Lookup helpers ────────────────────────────────────────────────────────

    private Project resolveProject(String nameOrId) {
        return projectRepository.findById(nameOrId)
                .or(() -> projectRepository.findAll().stream()
                        .filter(p -> nameOrId.equalsIgnoreCase(p.getName()))
                        .findFirst())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Project not found: '" + nameOrId + "'. Use --list-projects to see available projects."));
    }

    private String resolveSourceConnectionId(String nameOrId) {
        if (nameOrId == null || nameOrId.isBlank()) return null;
        return connectionRepository.findAll().stream()
                .filter(sc -> nameOrId.equals(sc.getId()) || nameOrId.equalsIgnoreCase(sc.getName()))
                .findFirst()
                .map(sc -> sc.getId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Source connection not found: '" + nameOrId + "'. Use --list-connections to see available connections."));
    }

    private String resolveMlConnectionId(String nameOrId) {
        return markLogicConnectionRepository.findAll().stream()
                .filter(sc -> nameOrId.equals(sc.getId()) || nameOrId.equalsIgnoreCase(sc.getName()))
                .findFirst()
                .map(sc -> sc.getId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "MarkLogic connection not found: '" + nameOrId + "'. Use --list-ml-connections to see available connections."));
    }

    // ── Terminal helpers ──────────────────────────────────────────────────────

    private void print(String msg) {
        System.out.println(msg);
    }

    private void printInPlace(String msg) {
        System.out.print("\r" + msg + "   ");
        System.out.flush();
    }

    private void clearLine() {
        System.out.print("\r" + " ".repeat(80) + "\r");
        System.out.flush();
    }

    private String formatElapsed(long seconds) {
        if (seconds < 60) return seconds + "s";
        if (seconds < 3600) return String.format("%dm%02ds", seconds / 60, seconds % 60);
        return String.format("%dh%02dm%02ds", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    }

    // ── ANSI colour constants ─────────────────────────────────────────────────

    private static final class Ansi {
        static final String RESET  = "\u001B[0m";
        static final String RED    = "\u001B[31m";
        static final String GREEN  = "\u001B[32m";
        static final String YELLOW = "\u001B[33m";
        static final String CYAN   = "\u001B[36m";
    }
}
