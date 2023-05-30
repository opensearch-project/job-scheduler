/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.request;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.FilterStreamInput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.jobscheduler.spi.JobExecutionContext;

/**
 * Request to extensions to invoke their ScheduledJobRunner implementation
 *
 */
public class JobRunnerRequest extends ActionRequest {

    /**
     * accessToken is the placeholder for the user Identity/access token to be used to perform validation prior to invoking the extension action
     */
    private final String accessToken;

    /**
     * jobParameterDocumentId is job index entry id
     */
    private final String jobParameterDocumentId;

    /**
     * jobExecutionContext holds the metadata to configure a job execution
     */
    private final JobExecutionContext jobExecutionContext;

    /**
     * Instantiates a new Job Runner Request
     *
     * @param accessToken the access token of this request
     * @param jobParameterDocumentId the document id of the job parameter
     * @param jobExecutionContext the context used to facilitate a job run
     */
    public JobRunnerRequest(String accessToken, String jobParameterDocumentId, JobExecutionContext jobExecutionContext) {
        this.accessToken = accessToken;
        this.jobParameterDocumentId = jobParameterDocumentId;
        this.jobExecutionContext = jobExecutionContext;
    }

    /**
     * Instantiates a new Job Runner Request from {@link StreamInput}
     *
     * @param in is the byte stream input used to de-serialize the message.
     * @throws IOException IOException when message de-serialization fails.
     */
    public JobRunnerRequest(StreamInput in) throws IOException {
        this.accessToken = in.readString();
        this.jobParameterDocumentId = in.readString();
        this.jobExecutionContext = new JobExecutionContext(in);
    }

    /**
     * Instantiates a new Job Runner Request by wrapping the given byte array within a {@link StreamInput}
     *
     * @param requestParams in bytes array used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public JobRunnerRequest(byte[] requestParams) throws IOException {
        this(FilterStreamInput.wrap(requestParams));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.accessToken);
        out.writeString(this.jobParameterDocumentId);
        this.jobExecutionContext.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getAccessToken() {
        return this.accessToken;
    }

    public String getJobParameterDocumentId() {
        return this.jobParameterDocumentId;
    }

    public JobExecutionContext getJobExecutionContext() {
        return this.jobExecutionContext;
    }

}
