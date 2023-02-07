/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.rest.request.GetJobDetailsRequest;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import static org.opensearch.rest.RestRequest.Method.PUT;
import org.opensearch.rest.RestStatus;

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
        return ImmutableList.of(
            new Route(PUT, String.format(Locale.ROOT, "%s/%s/{%s}", JobSchedulerPlugin.JS_BASE_URI, "_put/_release_lock/{lock_id}"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        String lockId = restRequest.param("lock_id");
        final LockModel[] lockModel = new LockModel[1];
        CompletableFuture<LockModel> findInProgressFuture = new CompletableFuture<>();
        lockService.findLock(lockId,ActionListener.wrap(
                lock->{
                    lockModel[0]=lock;
                    findInProgressFuture.complete(lock);
                },
                exception->{
                    logger.error("Could not find lock model with lockId " + lockId, exception);
                    findInProgressFuture.completeExceptionally(exception);
                }
        ));

        try {
            findInProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS).join();
            if(lockModel[0]==null){
                throw new IOException("Could not find lock model with lockId " + lockId);
            }
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.info(" Request timed out with an exception ", e);
            } else {
                throw e;
            }
        } catch (Exception e) {
            logger.info(" Could not find lock model with lockId due to exception ", e);
        }

        CompletableFuture<Boolean> releaseLockInProgressFuture = new CompletableFuture<>();

        final boolean[] releaseResponse = new boolean[1];
        lockService.release(lockModel[0], new ActionListener<>() {
            @Override
            public void onResponse(Boolean response) {
                releaseResponse[0] =response;
                releaseLockInProgressFuture.complete(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Releasing lock failed with an exception", e);
                releaseLockInProgressFuture.completeExceptionally(e);
            }
        });

        try {
            releaseLockInProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.info(" Request timed out with an exception ", e);
            } else {
                throw e;
            }
        } catch (Exception e) {
            logger.info(" Could not find lock model with lockId due to exception ", e);
        }


        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            String restResponseString = releaseResponse[0]? "success" : "failed";
            BytesRestResponse bytesRestResponse;
            try {
                builder.startObject();
                builder.field("response", restResponseString);
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
