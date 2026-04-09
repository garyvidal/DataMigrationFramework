package com.nativelogix.data.migration.framework.service.migration;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Spring Batch configuration for the cursor-driven migration pipeline.
 * <p>
 * Jobs are built dynamically per-run in {@link MigrationJobService}.
 * Two thread pools are provided:
 * <ul>
 *   <li>{@code asyncJobLauncher} — one thread per job (job itself is a single TaskletStep).</li>
 *   <li>{@code migrationDocBuilderExecutor} — N worker threads that pull root-row batches
 *       from the shared queue, batch-fetch child rows, and build documents concurrently.</li>
 * </ul>
 */
@Configuration
public class MigrationBatchConfig {

    @Bean(name = "asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        launcher.afterPropertiesSet();
        return launcher;
    }

    /**
     * Worker thread pool for the cursor pipeline document builders.
     * Each thread owns one JDBC child-fetch connection and processes batches independently.
     * Default: number of available CPUs, capped at the configured maximum.
     */
    @Bean(name = "migrationDocBuilderExecutor")
    public ThreadPoolTaskExecutor migrationDocBuilderExecutor(
            @Value("${migration.pipeline.worker-threads:0}") int configuredThreads) {
        int threads = configuredThreads > 0
                ? configuredThreads
                : Math.min(16, Runtime.getRuntime().availableProcessors());
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        // Queue capacity = workerThreadCount so that all workers can be pre-warmed
        // without blocking the Spring context during burst startup.
        executor.setQueueCapacity(threads);
        executor.setThreadNamePrefix("doc-builder-");
        executor.initialize();
        return executor;
    }
}
