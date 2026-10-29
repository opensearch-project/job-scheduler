/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.request;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class RunJobRequest extends BaseNodesRequest<RunJobRequest> {

    private final String jobType;
    private final String jobId;

    public RunJobRequest(String jobType, String jobId) {
        super(new String[0]);
        this.jobType = jobType;
        this.jobId = jobId;
    }

    public RunJobRequest(StreamInput in) throws IOException {
        super(in);
        this.jobType = in.readString();
        this.jobId = in.readString();
    }

    public String getJobType() {
        return jobType;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException exception = null;
        if (jobType == null || jobType.isEmpty()) {
            exception = ValidateActions.addValidationError("job_type is required", exception);
        }
        if (jobId == null || jobId.isEmpty()) {
            exception = ValidateActions.addValidationError("job_id is required", exception);
        }
        return exception;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jobType);
        out.writeString(jobId);
    }
}
