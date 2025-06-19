/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.GetScheduledInfoAction;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetScheduledInfoAction extends BaseRestHandler {

    private static final String GET_SCHEDULED_INFO_ACTION = "get_scheduled_info_action";

    @Override
    public String getName() {
        return GET_SCHEDULED_INFO_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, JobSchedulerPlugin.JS_BASE_URI + "/info"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetScheduledInfoRequest getScheduledInfoRequest = new GetScheduledInfoRequest();
        getScheduledInfoRequest.setByNode(request.paramAsBoolean("by_node", false));

        return channel -> client.execute(GetScheduledInfoAction.INSTANCE, getScheduledInfoRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(
                org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse response,
                XContentBuilder builder
            ) throws Exception {
                // Don't wrap in an additional object since response.toXContent already does that
                response.toXContent(builder, request);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
