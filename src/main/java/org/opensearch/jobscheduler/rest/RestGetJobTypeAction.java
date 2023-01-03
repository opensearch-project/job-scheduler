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
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.transport.GetJobTypeRequest;

import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;

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

    public JobDetailsService jobDetailsService;

    @Override
    public String getName() {
        return GET_JOB_TYPE_ACTION;
    }

    public RestGetJobTypeAction(Map<String, JobDetails> indexToJobDetails, final JobDetailsService jobDetailsService) {
        this.indexToJobDetails = indexToJobDetails;
        this.jobDetailsService = jobDetailsService;
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

        final String[] response = new String[1];
        final JobDetails[] updateJobDetailsResponse = new JobDetails[1];

        // JobDetails jobDetails = indexToJobDetails.getOrDefault(getJobTypeRequest.getExtensionId(), new JobDetails());
        // jobDetails.setJobType(getJobTypeRequest.getJobType());
        // indexToJobDetails.put(getJobTypeRequest.getExtensionId(), jobDetails);

        String jobType = getJobTypeRequest.getJobType();
        String extensionId = getJobTypeRequest.getExtensionId();

        ActionListener<JobDetails> actionListener = new ActionListener<JobDetails>() {
            @Override
            public void onResponse(JobDetails jobDetails) {
                if (jobDetails != null) {
                    response[0] = "success";
                    updateJobDetailsResponse[0] = jobDetails;
                } else {
                    response[0] = "failed";
                }
            }

            @Override
            public void onFailure(Exception e) {
                response[0] = "failed";
                logger.info("could not process job type", e);
            }
        };

        jobDetailsService.processJobIndexForExtensionId(
            null,
            jobType,
            null,
            null,
            extensionId,
            JobDetailsService.JobDetailsRequestType.JOB_TYPE,
            actionListener
        );

        // return channel -> client.execute(GetJobTypeAction.INSTANCE, getJobTypeRequest, getJobTypeResponse(channel, RestStatus.OK));
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", response[0]);
                if (response[0] == "success") {
                    builder.field("jobDetails", updateJobDetailsResponse[0]);
                } else {
                    restStatus = RestStatus.INTERNAL_SERVER_ERROR;
                }
                builder.endObject();
                bytesRestResponse = new BytesRestResponse(restStatus, builder);
            } finally {
                builder.close();
            }

            channel.sendResponse(bytesRestResponse);
        };
    }

    // private RestResponseListener<GetJobDetailsResponse> getJobTypeResponse(RestChannel channel, RestStatus status) {
    // return new RestResponseListener<>(channel) {
    // @Override
    // public RestResponse buildResponse(GetJobDetailsResponse response) throws Exception {
    // return new BytesRestResponse(status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
    // }
    // };
    // }
}
