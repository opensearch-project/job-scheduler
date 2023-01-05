/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.model.JobDetails;

import org.opensearch.jobscheduler.transport.GetJobIndexRequest;

import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * This class consists of the REST handler to GET job index from extensions.
 */
public class RestGetJobIndexAction extends BaseRestHandler {

    public static final String GET_JOB_INDEX_ACTION = "get_job_index_action";

    private final Logger logger = LogManager.getLogger(RestGetJobIndexAction.class);

    public JobDetailsService jobDetailsService;

    public RestGetJobIndexAction(final JobDetailsService jobDetailsService) {
        this.jobDetailsService = jobDetailsService;
    }

    @Override
    public String getName() {
        return GET_JOB_INDEX_ACTION;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(PUT, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_index")))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        GetJobIndexRequest getJobIndexRequest = GetJobIndexRequest.parse(parser);

        final JobDetails[] jobDetailsResponseHolder = new JobDetails[1];

        String jobIndex = getJobIndexRequest.getJobIndex();
        String jobParameterAction = getJobIndexRequest.getJobParameterAction();
        String jobRunnerAction = getJobIndexRequest.getJobRunnerAction();
        String extensionId = getJobIndexRequest.getExtensionId();

        CompletableFuture<JobDetails[]> inProgressFuture = new CompletableFuture<>();

        jobDetailsService.processJobDetailsForExtensionId(
            jobIndex,
            null,
            jobParameterAction,
            jobRunnerAction,
            extensionId,
            JobDetailsService.JobDetailsRequestType.JOB_INDEX,
            new ActionListener<>() {
                @Override
                public void onResponse(JobDetails jobDetails) {
                    jobDetailsResponseHolder[0] = jobDetails;
                    inProgressFuture.complete(jobDetailsResponseHolder);
                }

                @Override
                public void onFailure(Exception e) {
                    inProgressFuture.complete(null);
                    logger.info("could not process job index", e);
                }
            }
        );

        try {
            inProgressFuture.get(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.info("Time Limit Exceeded due to exception ", e);
        }

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponse = jobDetailsResponseHolder[0] != null ? "success" : "failed";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", restResponse);
                if (restResponse.equals("success")) {
                    builder.field("jobDetails", jobDetailsResponseHolder[0]);
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

}
