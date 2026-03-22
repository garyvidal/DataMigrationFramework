package com.nativelogix.rdbms2marklogic.service.migration;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

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
}
