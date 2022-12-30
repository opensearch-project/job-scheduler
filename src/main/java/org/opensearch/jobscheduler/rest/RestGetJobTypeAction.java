/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.transport.GetJobDetailsResponse;
import org.opensearch.jobscheduler.transport.GetJobTypeAction;
import org.opensearch.jobscheduler.transport.GetJobTypeRequest;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.BytesRestResponse;

import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * This class consists of the REST handler to GET job type from extensions.
 */
public class RestGetJobTypeAction extends BaseRestHandler {

    public static final String GET_JOB_TYPE_ACTION = "get_job_type_action";

    private final Logger logger = LogManager.getLogger(RestGetJobTypeAction.class);

    private Map<String, JobDetails> indexToJobDetails;

    @Override
    public String getName() {
        return GET_JOB_TYPE_ACTION;
    }

    public RestGetJobTypeAction(Map<String, JobDetails> indexToJobDetails) {
        this.indexToJobDetails = indexToJobDetails;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(PUT, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_type")))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        GetJobTypeRequest getJobTypeRequest = GetJobTypeRequest.parse(parser);

        JobDetails jobDetails = indexToJobDetails.getOrDefault(getJobTypeRequest.getExtensionId(), new JobDetails());
        jobDetails.setJobType(getJobTypeRequest.getJobType());
        indexToJobDetails.put(getJobTypeRequest.getExtensionId(), jobDetails);

        return channel -> client.execute(GetJobTypeAction.INSTANCE, getJobTypeRequest, getJobTypeResponse(channel, RestStatus.OK));
    }

    private RestResponseListener<GetJobDetailsResponse> getJobTypeResponse(RestChannel channel, RestStatus status) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetJobDetailsResponse response) throws Exception {
                return new BytesRestResponse(status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
