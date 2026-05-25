/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.RunJobAction;
import org.opensearch.jobscheduler.transport.request.RunJobRequest;
import org.opensearch.jobscheduler.transport.response.RunJobResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler that triggers an ad-hoc run of a scheduled job by job type and job id.
 */
public class RestRunJobAction extends BaseRestHandler {

    private static final String RUN_JOB_ACTION = "run_job_action";
    public static final String JOB_TYPE = "job_type";
    public static final String JOB_ID = "job_id";

    @Override
    public String getName() {
        return RUN_JOB_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, String.format(Locale.ROOT, "%s/_run/{%s}/{%s}", JobSchedulerPlugin.JS_BASE_URI, JOB_TYPE, JOB_ID)));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String jobType = request.param(JOB_TYPE);
        String jobId = request.param(JOB_ID);
        RunJobRequest runJobRequest = new RunJobRequest(jobType, jobId);

        return channel -> client.execute(RunJobAction.INSTANCE, runJobRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(RunJobResponse response, XContentBuilder builder) throws Exception {
                response.toXContent(builder, request);
                RestStatus status = response.getExecutingNode() != null ? RestStatus.OK : RestStatus.NOT_FOUND;
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
