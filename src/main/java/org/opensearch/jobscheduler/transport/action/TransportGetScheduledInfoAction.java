/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;

import java.io.IOException;
import java.util.List;

public class TransportGetScheduledInfoAction extends TransportNodesAction<
    GetScheduledInfoRequest,
    GetScheduledInfoResponse,
    GetScheduledInfoRequest.NodeRequest,
    GetScheduledInfoResponse.NodeResponse> {

    private static final Logger log = LogManager.getLogger(TransportGetScheduledInfoAction.class);
    private final JobScheduler jobScheduler;
    private static final DateFormatter STRICT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time");

    @Inject
    public TransportGetScheduledInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        JobScheduler jobScheduler
    ) {
        super(
            GetScheduledInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            GetScheduledInfoRequest::new,
            GetScheduledInfoRequest.NodeRequest::new,
            ThreadPool.Names.GENERIC,
            GetScheduledInfoResponse.NodeResponse.class
        );
        this.jobScheduler = jobScheduler;
    }

    @Override
    protected GetScheduledInfoResponse newResponse(
        GetScheduledInfoRequest request,
        List<GetScheduledInfoResponse.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetScheduledInfoResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected GetScheduledInfoRequest.NodeRequest newNodeRequest(GetScheduledInfoRequest request) {
        return new GetScheduledInfoRequest.NodeRequest();
    }

    @Override
    protected GetScheduledInfoResponse.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetScheduledInfoResponse.NodeResponse(in);
    }

    @Override
    protected GetScheduledInfoResponse.NodeResponse nodeOperation(GetScheduledInfoRequest.NodeRequest request) {
        GetScheduledInfoResponse.NodeResponse response = new GetScheduledInfoResponse.NodeResponse(clusterService.localNode());

        response.setJobs(jobScheduler.getJobsAsList());
        return response;
    }
}
