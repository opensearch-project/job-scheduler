/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class RestJobDetailsResponse extends ActionResponse implements ToXContentObject {

    private final RestStatus restStatus;

    public RestJobDetailsResponse(StreamInput in) throws IOException {
        super(in);
        restStatus = in.readEnum(RestStatus.class);
    }

    public RestJobDetailsResponse(String jobIndex, String jobParamAction, String jobRunnerAction, RestStatus restStatus) {
        this.restStatus=restStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return xContentBuilder
                .startObject()
                .endObject();
    }
}
