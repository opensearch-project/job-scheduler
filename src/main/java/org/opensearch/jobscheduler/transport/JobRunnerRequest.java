/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import java.io.IOException;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

/**
 * Request to extensions to invoke their ScheduledJobRunner implementation
 *
 */
public class JobRunnerRequest implements Writeable {

    /**
     * jobParameter is job index entry intended to be used to validate prior to job execution
     */
    private final ExtensionJobParameter jobParameter;

    /**
     * jobExecutionContext holds the metadata to configure a job execution
     */
    private final JobExecutionContext jobExecutionContext;

    /**
     * Instantiates a new Job Runner Request
     *
     * @param jobParameter the ScheduledJobParameter to convert into a writeable ExtensionJobParameter
     * @param jobExecutionContext the context used to facilitate a job run
     */
    public JobRunnerRequest(ScheduledJobParameter jobParameter, JobExecutionContext jobExecutionContext) {
        this.jobParameter = new ExtensionJobParameter(jobParameter);
        this.jobExecutionContext = jobExecutionContext;
    }

    /**
     * Instantiates a new Job Runner Request from {@link StreamInput}
     *
     * @param in in bytes stream input used to de-serialize the message.
     * @throws IOException IOException when message de-serialization fails.
     */
    public JobRunnerRequest(StreamInput in) throws IOException {
        this.jobParameter = new ExtensionJobParameter(in);
        this.jobExecutionContext = new JobExecutionContext(in);
    }

    /**
     * Instantiates a new Job Runner Request by wrapping the given byte array within a {@link StreamInput}
     *
     * @param requestParams in bytes array used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public JobRunnerRequest(byte[] requestParams) throws IOException {
        this(StreamInput.wrap(requestParams));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.jobParameter.writeTo(out);
        this.jobExecutionContext.writeTo(out);
    }

    public ExtensionJobParameter getJobParameter() {
        return this.jobParameter;
    }

    public JobExecutionContext getJobExecutionContext() {
        return this.jobExecutionContext;
    }
}
