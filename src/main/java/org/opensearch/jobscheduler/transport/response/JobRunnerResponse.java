/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response from extensions indicating the status of the ScheduledJobRunner invocation
 *
 */
public class JobRunnerResponse extends ActionResponse {

    /**
     * jobRunnerStatus indicates if the extension job runner has been executed
     */
    private final boolean jobRunnerStatus;

    /**
     * Instantiates a new Job Runner Response
     *
     * @param jobRunnerStatus the run status of the extension job runner
     */
    public JobRunnerResponse(boolean jobRunnerStatus) {
        this.jobRunnerStatus = jobRunnerStatus;
    }

    /**
     * Instantiates a new Job Runner Response from {@link StreamInput}
     *
     * @param in is the byte stream input used to de-serialize the message.
     * @throws IOException IOException when message de-serialization fails.
     */
    public JobRunnerResponse(StreamInput in) throws IOException {
        this.jobRunnerStatus = in.readBoolean();
    }

    /**
     * Instantiates a new Job Runner Response by wrapping the given byte array within a {@link StreamInput}
     *
     * @param responseParams in bytes array used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public JobRunnerResponse(byte[] responseParams) throws IOException {
        this(StreamInput.wrap(responseParams));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(this.jobRunnerStatus);
    }

    public boolean getJobRunnerStatus() {
        return this.jobRunnerStatus;
    }

}
