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

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;

/**
 * Response from extensions to parse a ScheduledJobParameter
 */
public class JobParameterResponse extends ActionResponse {

    /**
     * jobParameter is job index entry intended to be used to validate prior to job execution
     */
    private final ExtensionJobParameter jobParameter;

    /**
     * Instantiates a new Job Parameter Response
     *
     * @param jobParameter the job parameter parsed from the extension
     */
    public JobParameterResponse(ExtensionJobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * Instantiates a new Job Parameter Response from {@link StreamInput}
     *
     * @param in is the byte stream input used to de-serialize the message.
     * @throws IOException IOException when message de-serialization fails.
     */
    public JobParameterResponse(StreamInput in) throws IOException {
        this.jobParameter = new ExtensionJobParameter(in);
    }

    /**
     * Instantiates a new Job Parameter Response by wrapping the given byte array within a {@link StreamInput}
     *
     * @param responseParams in bytes array used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public JobParameterResponse(byte[] responseParams) throws IOException {
        this(StreamInput.wrap(responseParams));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.jobParameter.writeTo(out);
    }

    public ExtensionJobParameter getJobParameter() {
        return this.jobParameter;
    }
}
