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
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.GetHistoryAction;
import org.opensearch.jobscheduler.transport.request.GetHistoryRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for getting job history
 */
public class RestGetHistoryAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_history_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, JobSchedulerPlugin.JS_BASE_URI + "/api/history"),
            new Route(GET, JobSchedulerPlugin.JS_BASE_URI + "/api/history/{history_id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String history_id = request.param("history_id");
        GetHistoryRequest getHistoryRequest = new GetHistoryRequest(history_id);
        return channel -> client.execute(GetHistoryAction.INSTANCE, getHistoryRequest, new RestToXContentListener<>(channel));
    }
}
