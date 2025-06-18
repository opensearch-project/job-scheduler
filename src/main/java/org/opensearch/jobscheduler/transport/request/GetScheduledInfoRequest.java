/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.request;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetScheduledInfoRequest extends BaseNodesRequest<GetScheduledInfoRequest> {

    public GetScheduledInfoRequest() {
        super((new String[0]));
    }

    public GetScheduledInfoRequest(StreamInput in) throws IOException {
        super(in);
    }

    public GetScheduledInfoRequest(String... nodeIds) {
        super(nodeIds);
    }

    public GetScheduledInfoRequest(DiscoveryNode... nodes) {
        super(nodes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
