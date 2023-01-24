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
 */
public class JobRunnerRequest implements Writeable {

    private final ExtensionJobParameter jobParameter;
    private final JobExecutionContext jobExecutionContext;

    public JobRunnerRequest(ScheduledJobParameter jobParameter, JobExecutionContext jobExecutionContext) {
        this.jobParameter = new ExtensionJobParameter(jobParameter);
        this.jobExecutionContext = jobExecutionContext;
    }

    public JobRunnerRequest(StreamInput in) throws IOException {
        this.jobParameter = new ExtensionJobParameter(in);
        this.jobExecutionContext = new JobExecutionContext(in);
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
