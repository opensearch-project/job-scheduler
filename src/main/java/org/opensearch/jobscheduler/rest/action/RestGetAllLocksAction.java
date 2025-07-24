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
import org.opensearch.jobscheduler.transport.action.GetAllLocksAction;
import org.opensearch.jobscheduler.transport.request.GetAllLocksRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for getting all locks
 */
public class RestGetAllLocksAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_all_locks_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, JobSchedulerPlugin.JS_BASE_URI + "/api/locks"),
            new Route(GET, JobSchedulerPlugin.JS_BASE_URI + "/api/locks/{lock_id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String lockId = request.param("lock_id");
        GetAllLocksRequest getAllLocksRequest = new GetAllLocksRequest(lockId);
        return channel -> client.execute(GetAllLocksAction.INSTANCE, getAllLocksRequest, new RestToXContentListener<>(channel));
    }
}
