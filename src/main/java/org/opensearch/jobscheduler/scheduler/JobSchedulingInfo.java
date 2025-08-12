/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.scheduler;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.threadpool.Scheduler;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class JobSchedulingInfo implements Writeable, ToXContentObject {

    private static final Map<String, Function<StreamInput, ScheduledJobParameter>> PARAMETER_READERS = new ConcurrentHashMap<>();

    public static void registerParameterReader(String jobType, Function<StreamInput, ScheduledJobParameter> reader) {
        PARAMETER_READERS.put(jobType, reader);
    }

    private String indexName;
    private String jobType;
    private String jobId;
    private ScheduledJobParameter jobParameter;
    private boolean descheduled = false;
    private Instant actualPreviousExecutionTime;
    private Instant expectedPreviousExecutionTime;
    private Instant expectedExecutionTime;
    private Scheduler.ScheduledCancellable scheduledCancellable;

    public JobSchedulingInfo(ScheduledJobProvider provider, String jobId, ScheduledJobParameter jobParameter) {
        this.indexName = provider.getJobIndexName();
        this.jobType = provider.getJobType();
        this.jobId = jobId;
        this.jobParameter = jobParameter;
        registerParameterReader(this.jobType, jobParameter.getParameterReader());
    }

    public JobSchedulingInfo(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.jobType = in.readString();
        this.jobId = in.readString();

        // Use registry to deserialize the proper subclass
        Function<StreamInput, ScheduledJobParameter> reader = PARAMETER_READERS.get(jobType);
        if (reader != null) {
            this.jobParameter = reader.apply(in);
        } else {
            // Fallback to base class
            this.jobParameter = null;
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public String getJobId() {
        return jobId;
    }

    public ScheduledJobParameter getJobParameter() {
        return jobParameter;
    }

    public boolean isDescheduled() {
        return descheduled;
    }

    public Instant getActualPreviousExecutionTime() {
        return actualPreviousExecutionTime;
    }

    public Instant getExpectedPreviousExecutionTime() {
        return expectedPreviousExecutionTime;
    }

    public Instant getExpectedExecutionTime() {
        return this.expectedExecutionTime;
    }

    public Scheduler.ScheduledCancellable getScheduledCancellable() {
        return scheduledCancellable;
    }

    public void setDescheduled(boolean descheduled) {
        this.descheduled = descheduled;
    }

    public void setActualPreviousExecutionTime(Instant actualPreviousExecutionTime) {
        this.actualPreviousExecutionTime = actualPreviousExecutionTime;
    }

    public void setExpectedPreviousExecutionTime(Instant expectedPreviousExecutionTime) {
        this.expectedPreviousExecutionTime = expectedPreviousExecutionTime;
    }

    public void setExpectedExecutionTime(Instant expectedExecutionTime) {
        this.expectedExecutionTime = expectedExecutionTime;
    }

    public void setScheduledCancellable(Scheduler.ScheduledCancellable scheduledCancellable) {
        this.scheduledCancellable = scheduledCancellable;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeString(jobType);
        out.writeString(jobId);
        jobParameter.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index_name", indexName);
        builder.field("job_type", jobType);
        builder.field("job_id", jobId);
        builder.field("descheduled", descheduled);
        builder.field("actual_previous_execution_time", actualPreviousExecutionTime);
        builder.field("expected_previous_execution_time", expectedPreviousExecutionTime);
        builder.field("expected_execution_time", expectedExecutionTime);
        builder.field("job_parameter");
        jobParameter.toXContent(builder, params);

        // builder.field("job_parameter", jobParameter.toXContent(builder, params));
        builder.endObject();
        return builder;
    }
}
