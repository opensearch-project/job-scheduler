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
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

public class GetScheduledInfoResponse extends BaseNodesResponse<GetScheduledInfoResponse.NodeResponse> implements ToXContent {

    public GetScheduledInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetScheduledInfoResponse(
        ClusterName clusterName,
        List<GetScheduledInfoResponse.NodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<GetScheduledInfoResponse.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetScheduledInfoResponse.NodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetScheduledInfoResponse.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean byNode = params.paramAsBoolean("by_node", false);
        int totalJobs = 0;

        builder.startObject();

        if (byNode) {
            builder.startArray("nodes");
            for (GetScheduledInfoResponse.NodeResponse nodeResponse : getNodes()) {
                nodeResponse.toXContent(builder, params);
                totalJobs = totalJobs + nodeResponse.getJobs().size();
            }
            builder.endArray();
        } else {
            builder.startArray("jobs");
            Set<String> seenJobIds = new HashSet<>();
            for (GetScheduledInfoResponse.NodeResponse nodeResponse : getNodes()) {
                List<JobSchedulingInfo> jobs = nodeResponse.getJobs();
                for (JobSchedulingInfo job : jobs) {
                    if (seenJobIds.add(job.getJobId())) {
                        builder.value(job);
                        totalJobs++;
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

    public static class NodeResponse extends BaseNodeResponse implements ToXContentFragment {

        private List<JobSchedulingInfo> jobs;

        public NodeResponse(DiscoveryNode node) {
            super(node);
            this.jobs = new ArrayList<>();
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.jobs = in.readList(JobSchedulingInfo::new);
        }

        public List<JobSchedulingInfo> getJobs() {
            return jobs;
        }

        public void setJobs(List<JobSchedulingInfo> jobs) {
            this.jobs = jobs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(jobs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("node_id", getNode().getId());
            builder.field("node_name", getNode().getName());
            builder.field("jobs", jobs);
            builder.endObject();
            return builder;
        }

    }
}
