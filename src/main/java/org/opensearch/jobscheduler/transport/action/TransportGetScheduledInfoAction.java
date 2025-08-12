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
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    private void processJobsFromMap(
        Map<String, Map<String, JobSchedulingInfo>> jobInfoMap,
        Map<String, ScheduledJobProvider> indexToJobProvider,
        List<Map<String, Object>> jobs
    ) {
        for (Map.Entry<String, Map<String, JobSchedulingInfo>> indexEntry : jobInfoMap.entrySet()) {
            String indexName = indexEntry.getKey();
            Map<String, JobSchedulingInfo> jobsMap = indexEntry.getValue();

            if (jobsMap != null) {
                for (Map.Entry<String, JobSchedulingInfo> jobEntry : jobsMap.entrySet()) {
                    String jobId = jobEntry.getKey();
                    JobSchedulingInfo jobInfo = jobEntry.getValue();

                    if (jobInfo == null) {
                        log.debug("JobInfo {} does not exist.", jobId);
                        continue;
                    }

                    Map<String, Object> jobDetails = createJobDetails(jobInfo, jobId, indexName, indexToJobProvider);
                    jobs.add(jobDetails);
                }
            }
        }
    }

    private Map<String, Object> createJobDetails(
        JobSchedulingInfo jobInfo,
        String jobId,
        String indexName,
        Map<String, ScheduledJobProvider> indexToJobProvider
    ) {
        Map<String, Object> jobDetails = new LinkedHashMap<>();
        String jobType = indexToJobProvider.get(indexName).getJobType();

        jobDetails.put("job_type", jobType);
        jobDetails.put("job_id", jobId);
        jobDetails.put("index_name", indexName);
        jobDetails.put("name", jobInfo.getJobParameter().getName());
        jobDetails.put("descheduled", jobInfo.isDescheduled());
        jobDetails.put("enabled", jobInfo.getJobParameter().isEnabled());
        jobDetails.put(
            "enabled_time",
            STRICT_DATE_TIME_FORMATTER.format(jobInfo.getJobParameter().getEnabledTime().atOffset(ZoneOffset.UTC))
        );
        jobDetails.put(
            "last_update_time",
            STRICT_DATE_TIME_FORMATTER.format(jobInfo.getJobParameter().getLastUpdateTime().atOffset(ZoneOffset.UTC))
        );

        jobDetails.put(
            "last_execution_time",
            jobInfo.getActualPreviousExecutionTime() != null
                ? STRICT_DATE_TIME_FORMATTER.format(jobInfo.getActualPreviousExecutionTime().atOffset(ZoneOffset.UTC))
                : "none"
        );
        jobDetails.put(
            "last_expected_execution_time",
            jobInfo.getExpectedPreviousExecutionTime() != null
                ? STRICT_DATE_TIME_FORMATTER.format(jobInfo.getExpectedPreviousExecutionTime().atOffset(ZoneOffset.UTC))
                : "none"
        );
        jobDetails.put(
            "next_expected_execution_time",
            jobInfo.getExpectedExecutionTime() != null
                ? STRICT_DATE_TIME_FORMATTER.format(jobInfo.getExpectedExecutionTime().atOffset(ZoneOffset.UTC))
                : "none"
        );

        if (jobInfo.getJobParameter().getSchedule() != null) {
            jobDetails.put("schedule", createScheduleMap(jobInfo));
        }

        jobDetails.put(
            "lock_duration",
            jobInfo.getJobParameter().getLockDurationSeconds() != null ? jobInfo.getJobParameter().getLockDurationSeconds() : "no_lock"
        );
        jobDetails.put("jitter", jobInfo.getJobParameter().getJitter() != null ? jobInfo.getJobParameter().getJitter() : "none");

        return jobDetails;
    }

    private Map<String, Object> createScheduleMap(JobSchedulingInfo jobInfo) {
        Map<String, Object> scheduleMap = new LinkedHashMap<>();

        if (jobInfo.getJobParameter().getSchedule() instanceof IntervalSchedule intervalSchedule) {
            scheduleMap.put("type", IntervalSchedule.INTERVAL_FIELD);
            scheduleMap.put("start_time", STRICT_DATE_TIME_FORMATTER.format(intervalSchedule.getStartTime().atOffset(ZoneOffset.UTC)));
            scheduleMap.put("interval", intervalSchedule.getInterval());
            scheduleMap.put("unit", intervalSchedule.getUnit().toString());
            scheduleMap.put(
                "delay",
                jobInfo.getJobParameter().getSchedule().getDelay() != null ? jobInfo.getJobParameter().getSchedule().getDelay() : "none"
            );
        } else if (jobInfo.getJobParameter().getSchedule() instanceof CronSchedule cronSchedule) {
            scheduleMap.put("type", CronSchedule.CRON_FIELD);
            scheduleMap.put("expression", cronSchedule.getCronExpression());
            scheduleMap.put("timezone", cronSchedule.getTimeZone().getId());
            scheduleMap.put(
                "delay",
                jobInfo.getJobParameter().getSchedule().getDelay() != null ? jobInfo.getJobParameter().getSchedule().getDelay() : "none"
            );
        } else {
            scheduleMap.put("type", "unknown");
        }

        return scheduleMap;
    }
}
