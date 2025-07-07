/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import static org.opensearch.rest.RestRequest.Method.PUT;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.transport.client.node.NodeClient;

public class RestReleaseLockAction extends BaseRestHandler {

    public static final String RELEASE_LOCK_ACTION = "release_lock_action";
    private final Logger logger = LogManager.getLogger(RestReleaseLockAction.class);

    private LockService lockService;

    public RestReleaseLockAction(LockService lockService) {
        this.lockService = lockService;
    }

    @Override
    public String getName() {
        return RELEASE_LOCK_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(PUT, String.format(Locale.ROOT, "%s/%s/{%s}", JobSchedulerPlugin.JS_BASE_URI, "_release_lock", LockModel.LOCK_ID))
        );
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        String lockId = restRequest.param(LockModel.LOCK_ID);
        if (lockId == null || lockId.isEmpty()) {
            throw new IOException("lockId cannot be null or empty");
        }
        CompletableFuture<Boolean> releaseLockInProgressFuture = new CompletableFuture<>();
        if (!lockService.lockIndexExist()) {
            releaseLockInProgressFuture.complete(false);
        } else {
            CompletableFuture<LockModel> findInProgressFuture = new CompletableFuture<>();
            lockService.findLock(lockId, ActionListener.wrap(lock -> { findInProgressFuture.complete(lock); }, exception -> {
                logger.error("Could not find lock model with lockId " + lockId, exception);
                findInProgressFuture.completeExceptionally(exception);
            }));

            LockModel releaseLock;
            try {
                releaseLock = findInProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS).get();
            } catch (CompletionException | InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    logger.error(" Finding lock timed out ", e);
                }
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else if (e.getCause() instanceof Error) {
                    throw (Error) e.getCause();
                } else {
                    throw new RuntimeException(e.getCause());
                }
            }

            if (releaseLock != null) {
                lockService.release(releaseLock, new ActionListener<>() {
                    @Override
                    public void onResponse(Boolean response) {
                        releaseLockInProgressFuture.complete(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Releasing lock failed with an exception", e);
                        releaseLockInProgressFuture.completeExceptionally(e);
                    }
                });

                try {
                    releaseLockInProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
                } catch (CompletionException e) {
                    if (e.getCause() instanceof TimeoutException) {
                        logger.error("Release lock timed out ", e);
                    }
                    if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else if (e.getCause() instanceof Error) {
                        throw (Error) e.getCause();
                    } else {
                        throw new RuntimeException(e.getCause());
                    }
                }
            }
        }
        return channel -> {
            boolean releaseResponse = false;
            try {
                releaseResponse = releaseLockInProgressFuture.get();
            } catch (Exception e) {
                logger.error("Exception occured in releasing lock ", e);
            }
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponseString = releaseResponse ? "success" : "failed";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("release-lock", restResponseString);
                if (restResponseString.equals("failed")) {
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
