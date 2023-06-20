/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import com.google.common.collect.ImmutableList;
import static org.opensearch.rest.RestRequest.Method.GET;

import static org.opensearch.jobscheduler.spi.LockModel.GET_LOCK_ACTION;

/**
 * This class consists of the REST handler to GET a lock model for extensions
 */
public class RestRefreshTokenAction extends BaseRestHandler {
    private final Logger logger = LogManager.getLogger(RestRefreshTokenAction.class);

    public LockService lockService;

    public RestRefreshTokenAction(final LockService lockService) {
        this.lockService = lockService;
    }

    @Override
    public String getName() {
        return GET_LOCK_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_lock")));
    }

    @VisibleForTesting
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        return channel -> { channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Not implemented yet")); };
    }
}
