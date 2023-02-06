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

import org.opensearch.jobscheduler.transport.AcquireLockRequest;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
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
import com.google.common.collect.ImmutableList;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * This class consists of the REST handler to GET a lock model for extensions
 */
public class RestGetLockAction extends BaseRestHandler {

    private static final String GET_LOCK_ACTION = "get_lock_action";
    public static final String SEQUENCE_NUMBER = "seq_no";
    public static final String PRIMARY_TERM = "primary_term";
    public static final String LOCK_MODEL = "lock_model";

    private final Logger logger = LogManager.getLogger(RestGetLockAction.class);

    public LockService lockService;

    public RestGetLockAction(final LockService lockService) {
        this.lockService = lockService;
    }

    @Override
    public String getName() {
        return GET_LOCK_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_lock")));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        AcquireLockRequest acquireLockRequest = AcquireLockRequest.parse(parser);

        final LockModel[] lockModelResponseHolder = new LockModel[1];

        String jobId = acquireLockRequest.getJobId();
        String jobIndexName = acquireLockRequest.getJobIndexName();
        long lockDurationSeconds = acquireLockRequest.getLockDurationSeconds();

        CompletableFuture<LockModel[]> inProgressFuture = new CompletableFuture<>();

        lockService.acquireLockWithId(jobIndexName, lockDurationSeconds, jobIndexName, new ActionListener<>() {
            @Override
            public void onResponse(LockModel lockModel) {
                // set lockModelResponse
                lockModelResponseHolder[0] = lockModel;
                inProgressFuture.complete(lockModelResponseHolder);
            }

            @Override
            public void onFailure(Exception e) {
                logger.info("Could not acquire lock with ID : " + jobId, e);
                inProgressFuture.completeExceptionally(e);
            }
        });

        try {
            inProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.info(" Request timed out with an exception ", e);
            } else {
                throw e;
            }
        } catch (Exception e) {
            logger.info(" Could not process acquire lock request due to exception ", e);
        }

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponseString = lockModelResponseHolder[0] != null ? "success" : "failed";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", restResponseString);
                if (restResponseString.equals("success")) {
                    builder.field(LOCK_MODEL, lockModelResponseHolder[0].toString());
                    builder.field(SEQUENCE_NUMBER, lockModelResponseHolder[0].getSeqNo());
                    builder.field(PRIMARY_TERM, lockModelResponseHolder[0].getPrimaryTerm());
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
