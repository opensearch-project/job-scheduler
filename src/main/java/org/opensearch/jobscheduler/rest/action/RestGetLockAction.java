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

import static org.opensearch.jobscheduler.spi.LockModel.GET_LOCK_ACTION;
import static org.opensearch.jobscheduler.spi.LockModel.SEQUENCE_NUMBER;
import static org.opensearch.jobscheduler.spi.LockModel.PRIMARY_TERM;
import static org.opensearch.jobscheduler.spi.LockModel.LOCK_ID;
import static org.opensearch.jobscheduler.spi.LockModel.LOCK_MODEL;

/**
 * This class consists of the REST handler to GET a lock model for extensions
 */
public class RestGetLockAction extends BaseRestHandler {
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
        return ImmutableList.of(new Route(GET, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_lock")));
    }

    @VisibleForTesting
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        // Deserialize acquire lock request
        AcquireLockRequest acquireLockRequest = AcquireLockRequest.parse(parser);
        String jobId = acquireLockRequest.getJobId();
        String jobIndexName = acquireLockRequest.getJobIndexName();
        long lockDurationSeconds = acquireLockRequest.getLockDurationSeconds();

        // Process acquire lock request
        CompletableFuture<LockModel> inProgressFuture = new CompletableFuture<>();
        lockService.acquireLockWithId(
            jobIndexName,
            lockDurationSeconds,
            jobId,
            ActionListener.wrap(lockModel -> { inProgressFuture.complete(lockModel); }, exception -> {
                logger.info("Could not acquire lock with ID : " + jobId, exception);
                inProgressFuture.completeExceptionally(exception);
            })
        );

        try {
            inProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.error("Acquiring lock timed out ", e);
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
            BytesRestResponse bytesRestResponse;
            LockModel lockModelResponseHolder = null;
            try {
                lockModelResponseHolder = inProgressFuture.get();
            } catch (Exception e) {
                logger.error("Exception occured in acquiring lock ", e);
            }
            try (XContentBuilder builder = channel.newBuilder()) {
                // Prepare response
                RestStatus restStatus = RestStatus.OK;
                String restResponseString = lockModelResponseHolder != null ? "success" : "failed";

                builder.startObject();
                builder.field("response", restResponseString);
                if (restResponseString.equals("success")) {

                    // Prepare response fields
                    LockModel lock = lockModelResponseHolder;
                    long seqNo = lock.getSeqNo();
                    long primaryTerm = lock.getPrimaryTerm();

                    builder.field(LOCK_ID, LockModel.generateLockId(jobIndexName, jobId));
                    builder.field(LOCK_MODEL, lock);
                    builder.field(SEQUENCE_NUMBER, seqNo);
                    builder.field(PRIMARY_TERM, primaryTerm);
                } else {
                    restStatus = RestStatus.INTERNAL_SERVER_ERROR;
                }
                builder.endObject();
                bytesRestResponse = new BytesRestResponse(restStatus, builder);
                channel.sendResponse(bytesRestResponse);
            }
        };
    }
}
