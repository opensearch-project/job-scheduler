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

public class GetHistoryRequest extends ActionRequest {

    private String historyId;

    public GetHistoryRequest() {
        super();
    }

    public GetHistoryRequest(String historyId) {
        super();
        this.historyId = historyId;
    }

    public GetHistoryRequest(StreamInput in) throws IOException {
        super(in);
        this.historyId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(historyId);
    }

    public String getHistoryId() {
        return historyId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
