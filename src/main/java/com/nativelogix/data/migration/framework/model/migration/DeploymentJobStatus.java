package com.nativelogix.data.migration.framework.model.migration;

public enum DeploymentJobStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    /** Job finished but some documents failed to write; see {@code DeploymentJob#getErrors()}. */
    PARTIALLY_COMPLETED,
    FAILED,
    CANCELLED
}
