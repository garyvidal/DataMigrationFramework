package com.nativelogix.rdbms2marklogic.service.migration;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Minimal Spring Batch configuration — provides an async job launcher only.
 * <p>
 * Jobs and steps are built dynamically per-run in {@link MigrationJobService}
 * to carry per-job reader/writer instances without requiring step-scope proxies.
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
     * Thread pool used to run migration partitions in parallel.
     * Capped at 4 threads to avoid overwhelming the source RDBMS with connections.
     */
    @Bean(name = "migrationPartitionTaskExecutor")
    public ThreadPoolTaskExecutor migrationPartitionTaskExecutor() {
        int threads = Math.min(8, Runtime.getRuntime().availableProcessors());
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setThreadNamePrefix("migration-partition-");
        executor.initialize();
        return executor;
    }
}
