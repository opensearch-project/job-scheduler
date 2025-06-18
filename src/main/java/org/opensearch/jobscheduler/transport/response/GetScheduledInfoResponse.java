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
import java.util.Map;
import java.util.stream.Collectors;

public class GetScheduledInfoResponse extends BaseNodesResponse<GetScheduledInfoNodeResponse> implements ToXContent {

    public GetScheduledInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetScheduledInfoResponse(ClusterName clusterName, List<GetScheduledInfoNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<GetScheduledInfoNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetScheduledInfoNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetScheduledInfoNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("nodes");
        for (GetScheduledInfoNodeResponse nodeResponse : getNodes()) {
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

    public Map<String, Map<String, Object>> getScheduledJobInfoByNode() {
        return getNodes().stream()
            .collect(Collectors.toMap(node -> node.getNode().getId(), GetScheduledInfoNodeResponse::getScheduledJobInfo));
    }
}
