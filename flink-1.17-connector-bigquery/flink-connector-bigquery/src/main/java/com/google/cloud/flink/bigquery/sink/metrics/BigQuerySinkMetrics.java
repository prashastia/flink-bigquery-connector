package com.google.cloud.flink.bigquery.sink.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for Sink Metrics. */
public class BigQuerySinkMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkMetrics.class);

    // Constants
    public static final String BIGQUERY_SINK_METRIC_GROUP = "BigQuerySink";
    public static final String COMMITS_SUCCEEDED_METRIC_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRIC_COUNTER = "commitsFailed";

    // Writer reader metric group
    private final SinkWriterMetricGroup sinkWriterMetricGroup;

    // Metric group for registering BigQuery specific metrics
    private final MetricGroup bigQuerySinkMetricGroup;

    // Successful / Failed commits counters
    private final Counter commitsSucceeded;
    private final Counter commitsFailed;

    public BigQuerySinkMetrics(SinkWriterMetricGroup sinkWriterMetricGroup) {
        this.sinkWriterMetricGroup = sinkWriterMetricGroup;
        this.bigQuerySinkMetricGroup =
                this.sinkWriterMetricGroup.addGroup(BIGQUERY_SINK_METRIC_GROUP);
        this.commitsSucceeded =
                this.bigQuerySinkMetricGroup.counter(COMMITS_SUCCEEDED_METRIC_COUNTER);
        this.commitsFailed = this.bigQuerySinkMetricGroup.counter(COMMITS_FAILED_METRIC_COUNTER);
        System.out.println("Intitalised!");
    }

    /** Mark a successful commit. */
    public void recordSucceededCommit() {
        System.out.println("recordSucceededCommit!");
        this.commitsSucceeded.inc();
    }

    /** Mark a failure commit. */
    public void recordFailedCommit() {
        System.out.println("recordFailedCommit!");
        this.commitsFailed.inc();
    }
}
