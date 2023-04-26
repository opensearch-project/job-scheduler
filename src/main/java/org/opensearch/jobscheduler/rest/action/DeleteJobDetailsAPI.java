/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.rest.action;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.rest.request.DeleteJobDetailsRequest;
import org.opensearch.jobscheduler.rest.request.GetJobDetailsRequest;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.jobscheduler.rest.request.DeleteJobDetailsRequest.DOCUMENT_ID;
import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class DeleteJobDetailsAPI extends BaseRestHandler {

    public static final String DELETE_JOB_DETAILS_API = "delete_job_details_api";

    private final Logger logger = LogManager.getLogger(RestGetJobDetailsAction.class);

    public JobDetailsService jobDetailsService;

    public DeleteJobDetailsAPI(final JobDetailsService jobDetailsService) {
        this.jobDetailsService = jobDetailsService;
    }

    @Override
    public String getName() {
        return DELETE_JOB_DETAILS_API;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
                // Delete Job Details Entry Request
                new Route(
                        DELETE,
                        String.format(Locale.ROOT, "%s/%s/{%s}", JobSchedulerPlugin.JS_BASE_URI, "_job_details", DOCUMENT_ID)
                )

        );
    }

    @VisibleForTesting
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        DeleteJobDetailsRequest deleteJobDetailsRequest = DeleteJobDetailsRequest.parse(parser);
        String documentId = restRequest.param(DOCUMENT_ID);

        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();

        jobDetailsService.deleteJobDetails(
                documentId,
                new ActionListener<>() {

                    @Override
                    public void onResponse(Boolean aBoolean) {
                        inProgressFuture.complete(aBoolean);
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
                logger.error("Delete Job Details timed out ", e);
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
            Boolean jobDetailsResponseHolder = false;
            try {
                jobDetailsResponseHolder = inProgressFuture.get();
            } catch (Exception e) {
                logger.error("Exception occured in get job details ", e);
            }
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponseString = jobDetailsResponseHolder != false ? "DELETED" : "NOT_FOUND";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", restResponseString);
                if (restResponseString.equals("DELETED")) {
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
