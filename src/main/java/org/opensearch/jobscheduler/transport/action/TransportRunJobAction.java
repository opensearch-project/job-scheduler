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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.request.RunJobNodeRequest;
import org.opensearch.jobscheduler.transport.request.RunJobRequest;
import org.opensearch.jobscheduler.transport.response.RunJobNodeResponse;
import org.opensearch.jobscheduler.transport.response.RunJobResponse;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class TransportRunJobAction extends TransportNodesAction<RunJobRequest, RunJobResponse, RunJobNodeRequest, RunJobNodeResponse> {

    private static final Logger log = LogManager.getLogger(TransportRunJobAction.class);

    private final JobScheduler jobScheduler;
    private final JobDetailsService jobDetailsService;
    private final LockService lockService;

    @Inject
    public TransportRunJobAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        JobScheduler jobScheduler,
        JobDetailsService jobDetailsService,
        LockService lockService
    ) {
        super(
            RunJobAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            RunJobRequest::new,
            RunJobNodeRequest::new,
            ThreadPool.Names.GENERIC,
            RunJobNodeResponse.class
        );
        this.jobScheduler = jobScheduler;
        this.jobDetailsService = jobDetailsService;
        this.lockService = lockService;
    }

    @Override
    protected RunJobResponse newResponse(
        RunJobRequest request,
        List<RunJobNodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new RunJobResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected RunJobNodeRequest newNodeRequest(RunJobRequest request) {
        return new RunJobNodeRequest(request);
    }

    @Override
    protected RunJobNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new RunJobNodeResponse(in);
    }

    @Override
    protected RunJobNodeResponse nodeOperation(RunJobNodeRequest request) {
        String jobType = request.getJobType();
        String jobId = request.getJobId();

        ScheduledJobProvider provider = resolveProvider(jobType);
        if (provider == null) {
            return new RunJobNodeResponse(clusterService.localNode(), false, "no provider registered for job_type [" + jobType + "]");
        }

        String indexName = provider.getJobIndexName();
        JobSchedulingInfo jobSchedulingInfo = jobScheduler.getScheduledJobInfo().getJobInfo(indexName, jobId);
        if (jobSchedulingInfo == null) {
            return new RunJobNodeResponse(clusterService.localNode(), false, "job not scheduled on this node");
        }

        ScheduledJobParameter jobParameter = jobSchedulingInfo.getJobParameter();
        ScheduledJobRunner jobRunner = provider.getJobRunner();
        if (jobRunner == null) {
            return new RunJobNodeResponse(clusterService.localNode(), false, "no runner registered for job_type [" + jobType + "]");
        }

        try {
            JobExecutionContext context = new JobExecutionContext(
                Instant.now(),
                new JobDocVersion(0L, 0L, 0L),
                lockService,
                indexName,
                jobId
            );
            log.info("Ad-hoc triggering job id {} for index {}", jobId, indexName);
            jobRunner.runJob(jobParameter, context);
            return new RunJobNodeResponse(clusterService.localNode(), true, null);
        } catch (Exception e) {
            log.error("Failed to ad-hoc trigger job id " + jobId + " for index " + indexName, e);
            return new RunJobNodeResponse(clusterService.localNode(), false, "execution failed: " + e.getMessage());
        }
    }

    private ScheduledJobProvider resolveProvider(String jobType) {
        Map<String, ScheduledJobProvider> providers = jobDetailsService.getIndexToJobProviders();
        for (ScheduledJobProvider provider : providers.values()) {
            if (provider.getJobType().equals(jobType)) {
                return provider;
            }
        }
        return null;
    }
}
