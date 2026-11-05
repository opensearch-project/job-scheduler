/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.request;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

public class GetHistoryRequest extends ActionRequest {

    private String jobIndexName;
    private String jobId;

    public GetHistoryRequest() {
        super();
    }

    public GetHistoryRequest(String jobIndexName, String jobId) {
        super();
        this.jobIndexName = jobIndexName;
        this.jobId = jobId;
    }

    public GetHistoryRequest(StreamInput in) throws IOException {
        super(in);
        this.jobIndexName = in.readOptionalString();
        this.jobId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(jobIndexName);
        out.writeOptionalString(jobId);
    }

    public String getJobIndexName() {
        return jobIndexName;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((jobIndexName == null) != (jobId == null)) {
            validationException = addValidationError(
                "job_index_name and job_id must both be provided to filter job history",
                validationException
            );
        }
        return validationException;
    }
}
