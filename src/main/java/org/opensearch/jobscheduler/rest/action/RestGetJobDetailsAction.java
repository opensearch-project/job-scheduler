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
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;

import org.opensearch.jobscheduler.rest.request.GetJobDetailsRequest;

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
            new Route(PUT, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_job_details")),
            // Update Job Details Entry Request
            new Route(
                PUT,
                String.format(Locale.ROOT, "%s/%s/{%s}", JobSchedulerPlugin.JS_BASE_URI, "_job_details", GetJobDetailsRequest.DOCUMENT_ID)
            )

        );
    }

    @VisibleForTesting
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        GetJobDetailsRequest getJobDetailsRequest = GetJobDetailsRequest.parse(parser);
        String documentId = restRequest.param(GetJobDetailsRequest.DOCUMENT_ID);
        String jobIndex = getJobDetailsRequest.getJobIndex();
        String jobType = getJobDetailsRequest.getJobType();
        String jobParameterAction = getJobDetailsRequest.getJobParameterAction();
        String jobRunnerAction = getJobDetailsRequest.getJobRunnerAction();
        String extensionUniqueId = getJobDetailsRequest.getExtensionUniqueId();

        CompletableFuture<String> inProgressFuture = new CompletableFuture<>();

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
                    inProgressFuture.complete(indexedDocumentId);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.info("could not process job index", e);
                    inProgressFuture.completeExceptionally(e);
                }
            }
        );

        try {
            inProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.error("Get Job Details timed out ", e);
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }

        return channel -> {
            String jobDetailsResponseHolder = null;
            try {
                jobDetailsResponseHolder = inProgressFuture.get();
            } catch (Exception e) {
                logger.error("Exception occured in get job details ", e);
            }
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponseString = jobDetailsResponseHolder != null ? "success" : "failed";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", restResponseString);
                if (restResponseString.equals("success")) {
                    builder.field(GetJobDetailsRequest.DOCUMENT_ID, jobDetailsResponseHolder);
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
