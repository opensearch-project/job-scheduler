/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

public class RunJobNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private final boolean executed;
    private final String message;

    public RunJobNodeResponse(DiscoveryNode node, boolean executed, String message) {
        super(node);
        this.executed = executed;
        this.message = message;
    }

    public RunJobNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.executed = in.readBoolean();
        this.message = in.readOptionalString();
    }

    public boolean isExecuted() {
        return executed;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(executed);
        out.writeOptionalString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node_id", getNode().getId());
        builder.field("node_name", getNode().getName());
        builder.field("executed", executed);
        if (message != null) {
            builder.field("message", message);
        }
        builder.endObject();
        return builder;
    }
}
