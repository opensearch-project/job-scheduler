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
import java.util.HashMap;
import java.util.Map;

public class GetScheduledInfoNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private Map<String, Object> scheduledJobInfo;

    public GetScheduledInfoNodeResponse(DiscoveryNode node) {
        super(node);
        this.scheduledJobInfo = new HashMap<>();
    }

    public GetScheduledInfoNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.scheduledJobInfo = in.readMap();
    }

    public Map<String, Object> getScheduledJobInfo() {
        return scheduledJobInfo;
    }

    public void setScheduledJobInfo(Map<String, Object> scheduledJobInfo) {
        this.scheduledJobInfo = scheduledJobInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(scheduledJobInfo);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node_id", getNode().getId());
        builder.field("node_name", getNode().getName());
        builder.field("scheduled_job_info", scheduledJobInfo);
        builder.endObject();
        return builder;
    }

}
