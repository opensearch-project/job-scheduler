/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class RunJobResponse extends BaseNodesResponse<RunJobNodeResponse> implements ToXContent {

    public RunJobResponse(StreamInput in) throws IOException {
        super(in);
    }

    public RunJobResponse(ClusterName clusterName, List<RunJobNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<RunJobNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(RunJobNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<RunJobNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    /**
     * Returns the node response that actually executed the job, or null if no node did.
     */
    public RunJobNodeResponse getExecutingNode() {
        for (RunJobNodeResponse node : getNodes()) {
            if (node.isExecuted()) {
                return node;
            }
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        RunJobNodeResponse executingNode = getExecutingNode();
        builder.startObject();
        builder.field("executed", executingNode != null);
        if (executingNode != null) {
            builder.field("executing_node_id", executingNode.getNode().getId());
            builder.field("executing_node_name", executingNode.getNode().getName());
        }
        builder.startArray("nodes");
        for (RunJobNodeResponse nodeResponse : getNodes()) {
            nodeResponse.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray("failures");
        for (FailedNodeException failure : failures()) {
            builder.startObject();
            builder.field("node_id", failure.nodeId());
            builder.field("reason", failure.getMessage());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
