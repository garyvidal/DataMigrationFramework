package com.nativelogix.data.migration.framework.model.migration;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of a dry-run sample execution. The full pipeline (cursor → child fetch →
 * document build → transform) is exercised on a small sample of rows, but nothing
 * is written to MarkLogic. Returned synchronously by the dry-run-sample endpoint.
 */
public class DryRunReport {

    /** Number of root rows sampled. */
    private int sampleSize;
    /** Built document content (XML or JSON strings), up to {@link #sampleSize} entries. */
    private List<String> sampleDocuments = new ArrayList<>();
    /** Pipeline errors encountered during the sample run (transform failures, missing fields, etc.). */
    private List<String> pipelineErrors = new ArrayList<>();
    /** Estimated throughput in rows/second based on sample timing. 0 if sample was instant. */
    private double estimatedRowsPerSecond;
    /** Elapsed milliseconds for the sample run. */
    private long elapsedMillis;
    /** Total root records available in the source table (from COUNT query). */
    private long totalSourceRecords;

    public DryRunReport() {}

    public int getSampleSize() { return sampleSize; }
    public void setSampleSize(int sampleSize) { this.sampleSize = sampleSize; }

    public List<String> getSampleDocuments() { return sampleDocuments; }
    public void setSampleDocuments(List<String> sampleDocuments) { this.sampleDocuments = sampleDocuments; }

    public List<String> getPipelineErrors() { return pipelineErrors; }
    public void setPipelineErrors(List<String> pipelineErrors) { this.pipelineErrors = pipelineErrors; }

    public double getEstimatedRowsPerSecond() { return estimatedRowsPerSecond; }
    public void setEstimatedRowsPerSecond(double estimatedRowsPerSecond) { this.estimatedRowsPerSecond = estimatedRowsPerSecond; }

    public long getElapsedMillis() { return elapsedMillis; }
    public void setElapsedMillis(long elapsedMillis) { this.elapsedMillis = elapsedMillis; }

    public long getTotalSourceRecords() { return totalSourceRecords; }
    public void setTotalSourceRecords(long totalSourceRecords) { this.totalSourceRecords = totalSourceRecords; }
}
