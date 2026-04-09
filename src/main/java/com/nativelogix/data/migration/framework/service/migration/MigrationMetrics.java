package com.nativelogix.data.migration.framework.service.migration;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central Micrometer metrics registry for the data migration pipeline.
 *
 * <h3>Metric groups</h3>
 * <ul>
 *   <li><b>db.*</b>  — RDBMS query timing (root cursor open, batch child fetches, cache load)</li>
 *   <li><b>reader.*</b> — Prefetch throughput and rows read rate</li>
 *   <li><b>build.*</b>  — XML / JSON document build latency</li>
 *   <li><b>ml.*</b>     — MarkLogic write latency and document throughput</li>
 *   <li><b>job.*</b>    — Active jobs and partition counts</li>
 * </ul>
 *
 * Metrics are exported at {@code /actuator/prometheus} (Prometheus text format)
 * and {@code /actuator/metrics} (JSON).
 */
@Component
public class MigrationMetrics {

    // ── RDBMS / source reading ────────────────────────────────────────────────

    /** Time to open + execute the root cursor ({@code open()} phase). */
    public final Timer dbRootCursorOpenTimer;

    /** Time per batch IN-query for non-cached child tables. */
    public final Timer dbChildBatchFetchTimer;

    /** Time to load one lookup-cache table entirely into memory. */
    public final Timer dbLookupCacheLoadTimer;

    /** Total rows read from the RDBMS source across all jobs. */
    public final Counter rowsReadCounter;

    /** Rows read per prefetch batch (distribution summary = histogram). */
    public final DistributionSummary prefetchBatchSizeHistogram;

    // ── Document building ─────────────────────────────────────────────────────

    /** Time to build one XML document string. */
    public final Timer xmlBuildTimer;

    /** Time to build one JSON document string. */
    public final Timer jsonBuildTimer;

    // ── MarkLogic writes ──────────────────────────────────────────────────────

    /** Time per {@code write()} call (chunk write round-trip). */
    public final Timer mlWriteTimer;

    /** Total documents successfully written to MarkLogic. */
    public final Counter mlDocsWrittenCounter;

    /** Total documents that failed to write (async batcher failures). */
    public final Counter mlWriteErrorCounter;

    // ── Job-level gauges ──────────────────────────────────────────────────────

    private final AtomicLong activeJobs = new AtomicLong(0);
    private final AtomicLong activePartitions = new AtomicLong(0);

    // ── Constructor ───────────────────────────────────────────────────────────

    public MigrationMetrics(MeterRegistry registry) {

        dbRootCursorOpenTimer = Timer.builder("db.root_cursor.open")
                .description("Time to open and execute the root RDBMS cursor")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        dbChildBatchFetchTimer = Timer.builder("db.child.batch_fetch")
                .description("Time for a single batch IN-query child fetch")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        dbLookupCacheLoadTimer = Timer.builder("db.lookup_cache.load")
                .description("Time to load one small child table into the in-memory lookup cache")
                .register(registry);

        rowsReadCounter = Counter.builder("reader.rows_read")
                .description("Total root rows read from the RDBMS source")
                .register(registry);

        prefetchBatchSizeHistogram = DistributionSummary.builder("reader.prefetch_batch_size")
                .description("Number of root rows processed per prefetch batch")
                .publishPercentiles(0.5, 0.95)
                .register(registry);

        xmlBuildTimer = Timer.builder("build.xml.document")
                .description("Time to build one XML document string")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        jsonBuildTimer = Timer.builder("build.json.document")
                .description("Time to build one JSON document string")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        mlWriteTimer = Timer.builder("ml.write.chunk")
                .description("Time for one Spring Batch chunk write to MarkLogic")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        mlDocsWrittenCounter = Counter.builder("ml.docs.written")
                .description("Total documents successfully written to MarkLogic")
                .register(registry);

        mlWriteErrorCounter = Counter.builder("ml.write.errors")
                .description("Total MarkLogic write errors (async batcher failures)")
                .register(registry);

        Gauge.builder("job.active", activeJobs, AtomicLong::doubleValue)
                .description("Number of migration jobs currently running")
                .register(registry);

        Gauge.builder("job.active_partitions", activePartitions, AtomicLong::doubleValue)
                .description("Number of active partition steps currently running")
                .register(registry);
    }

    // ── Convenience helpers ───────────────────────────────────────────────────

    public void jobStarted()   { activeJobs.incrementAndGet(); }
    public void jobFinished()  { activeJobs.decrementAndGet(); }

    public void partitionStarted()  { activePartitions.incrementAndGet(); }
    public void partitionFinished() { activePartitions.decrementAndGet(); }

    /** Record a timing in nanoseconds directly (for use in try-finally blocks). */
    public void recordNanos(Timer timer, long nanos) {
        timer.record(nanos, TimeUnit.NANOSECONDS);
    }
}
