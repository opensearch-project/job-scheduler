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

public class GetScheduledInfoNodeRequest extends ActionRequest {

    public GetScheduledInfoNodeRequest() {
        super();
    }

    public GetScheduledInfoNodeRequest(StreamInput in) throws IOException {
        super(in);
    }

    public GetScheduledInfoNodeRequest(GetScheduledInfoRequest request) {
        super();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
