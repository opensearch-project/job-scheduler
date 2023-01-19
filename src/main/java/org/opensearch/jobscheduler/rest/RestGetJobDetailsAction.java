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

import org.opensearch.jobscheduler.transport.GetJobDetailsRequest;

import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableList;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * This class consists of the REST handler to GET job details from extensions.
 */
public class RestGetJobDetailsAction extends BaseRestHandler {

    public static final String GET_JOB_DETAILS_ACTION = "get_job_details_action";

    private final Logger logger = LogManager.getLogger(RestGetJobDetailsAction.class);

    public JobDetailsService jobDetailsService;

    public RestGetJobDetailsAction(final JobDetailsService jobDetailsService) {
        this.jobDetailsService = jobDetailsService;
    }

    @Override
    public String getName() {
        return GET_JOB_DETAILS_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            // New Job Details Entry Request
            new Route(PUT, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_details")),
            // Update Job Details Entry Request
            new Route(
                PUT,
                String.format(
                    Locale.ROOT,
                    "%s/%s/{%s}",
                    JobSchedulerPlugin.JS_BASE_URI,
                    "_get/_job_details",
                    GetJobDetailsRequest.DOCUMENT_ID
                )
            )

        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        GetJobDetailsRequest getJobDetailsRequest = GetJobDetailsRequest.parse(parser);

        final String[] jobDetailsResponseHolder = new String[1];

        String documentId = restRequest.param(GetJobDetailsRequest.DOCUMENT_ID);
        String jobIndex = getJobDetailsRequest.getJobIndex();
        String jobType = getJobDetailsRequest.getJobType();
        String jobParameterAction = getJobDetailsRequest.getJobParameterAction();
        String jobRunnerAction = getJobDetailsRequest.getJobRunnerAction();
        String extensionUniqueId = getJobDetailsRequest.getExtensionUniqueId();

        CompletableFuture<String[]> inProgressFuture = new CompletableFuture<>();

        jobDetailsService.processJobDetails(
            documentId,
            jobIndex,
            jobType,
            jobParameterAction,
            jobRunnerAction,
            extensionUniqueId,
            new ActionListener<>() {
                @Override
                public void onResponse(String indexedDocumentId) {
                    // Set document Id
                    jobDetailsResponseHolder[0] = indexedDocumentId;
                    inProgressFuture.complete(jobDetailsResponseHolder);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.info("could not process job index", e);
                    inProgressFuture.completeExceptionally(e);
                }
            }
        );

        try {
            inProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.info(" Request timed out with an exception ", e);
            } else {
                throw e;
            }
        } catch (Exception e) {
            logger.info(" Could not process job index due to exception ", e);
        }

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponseString = jobDetailsResponseHolder[0] != null ? "success" : "failed";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", restResponseString);
                if (restResponseString.equals("success")) {
                    builder.field(GetJobDetailsRequest.DOCUMENT_ID, jobDetailsResponseHolder[0]);
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
