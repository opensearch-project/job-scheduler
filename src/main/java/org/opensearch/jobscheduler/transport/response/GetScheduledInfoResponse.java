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
import java.util.Set;
import java.util.HashSet;
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
        boolean byNode = params.paramAsBoolean("by_node", false);
        int totalJobs = 0;

        builder.startObject();

        if (byNode) {
            builder.startArray("nodes");
            for (GetScheduledInfoNodeResponse nodeResponse : getNodes()) {
                nodeResponse.toXContent(builder, params);
                totalJobs = totalJobs + (int) nodeResponse.getScheduledJobInfo().get("total_jobs");
            }
            builder.endArray();
        } else {
            builder.startArray("jobs");
            Set<Object> uniqueJobs = new HashSet<>();
            for (GetScheduledInfoNodeResponse nodeResponse : getNodes()) {
                Object jobs = nodeResponse.getScheduledJobInfo().get("jobs");
                if (jobs instanceof List) {
                    for (Object job : (List<?>) jobs) {
                        if (uniqueJobs.add(job)) {
                            builder.value(job);
                            totalJobs++;
                        }
                    }
                }
            }
            builder.endArray();
        }

        builder.startArray("failures");
        for (FailedNodeException failure : failures()) {
            builder.startObject();
            builder.field("node_id", failure.nodeId());
            builder.field("reason", failure.getMessage());
            builder.endObject();
        }
        builder.endArray();
        builder.field("total_jobs", totalJobs);
        builder.endObject();
        return builder;
    }

    public Map<String, Map<String, Object>> getScheduledJobInfoByNode() {
        return getNodes().stream()
            .collect(Collectors.toMap(node -> node.getNode().getId(), GetScheduledInfoNodeResponse::getScheduledJobInfo));
    }
}
