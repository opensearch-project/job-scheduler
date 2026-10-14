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

public class RunJobNodeRequest extends ActionRequest {

    private final String jobType;
    private final String jobId;

    public RunJobNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.jobType = in.readString();
        this.jobId = in.readString();
    }

    public RunJobNodeRequest(RunJobRequest request) {
        super();
        this.jobType = request.getJobType();
        this.jobId = request.getJobId();
    }

    public String getJobType() {
        return jobType;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jobType);
        out.writeString(jobId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
