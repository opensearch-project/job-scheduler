/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class GetJobDetailsResponse extends ActionResponse implements ToXContentObject {

    private final RestStatus restStatus;
    private final String response;
    private final static String RESPONSE = "response";

    public GetJobDetailsResponse(StreamInput in) throws IOException {
        super(in);
        restStatus = in.readEnum(RestStatus.class);
        response = in.readString();
    }

    public GetJobDetailsResponse(RestStatus restStatus, String response) {
        this.restStatus = restStatus;
        this.response = response;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(restStatus);
    }

    public RestStatus getRestStatus() {
        return restStatus;
    }

    public String getResponse() {
        return response;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return xContentBuilder.startObject().field(RESPONSE, response).endObject();
    }
}
