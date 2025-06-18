/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;
import org.opensearch.jobscheduler.scheduler.ScheduledJobInfo;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoNodeRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoNodeResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportGetScheduledInfoAction extends TransportNodesAction<
    GetScheduledInfoRequest,
    GetScheduledInfoResponse,
    GetScheduledInfoNodeRequest,
    GetScheduledInfoNodeResponse> {

    private final JobScheduler jobScheduler;

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
            GetScheduledInfoNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            GetScheduledInfoNodeResponse.class
        );
        this.jobScheduler = jobScheduler;
    }

    @Override
    protected GetScheduledInfoResponse newResponse(
        GetScheduledInfoRequest request,
        List<GetScheduledInfoNodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetScheduledInfoResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected GetScheduledInfoNodeRequest newNodeRequest(GetScheduledInfoRequest request) {
        return new GetScheduledInfoNodeRequest(request);
    }

    @Override
    protected GetScheduledInfoNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetScheduledInfoNodeResponse(in);
    }

    @Override
    protected GetScheduledInfoNodeResponse nodeOperation(GetScheduledInfoNodeRequest request) {
        GetScheduledInfoNodeResponse response = new GetScheduledInfoNodeResponse(clusterService.localNode());
        Map<String, Object> scheduledJobInfo = new HashMap<>();

        try {
            // Create a list to hold all job details
            List<Map<String, Object>> jobs = new java.util.ArrayList<>();

            // Get scheduled job information from the job scheduler
            if (jobScheduler != null) {
                ScheduledJobInfo scheduledJobInfoLocal = jobScheduler.getScheduledJobInfo();

                if (scheduledJobInfoLocal != null && scheduledJobInfoLocal.getJobInfoMap() != null) {
                    for (Map.Entry<String, Map<String, JobSchedulingInfo>> indexEntry : scheduledJobInfoLocal.getJobInfoMap().entrySet()) {
                        String indexName = indexEntry.getKey();
                        Map<String, JobSchedulingInfo> jobsMap = indexEntry.getValue();

                        if (jobsMap != null) {
                            for (Map.Entry<String, JobSchedulingInfo> jobEntry : jobsMap.entrySet()) {
                                String jobId = jobEntry.getKey();
                                JobSchedulingInfo jobInfo = jobEntry.getValue();

                                if (jobInfo == null) continue;

                                Map<String, Object> jobDetails = new HashMap<>();

                                // Get job provider type if available
                                String jobType = "unknown";
                                /*try {
                                    if (jobScheduler.getJobProviderByIndex(indexName) != null) {
                                    jobType = jobScheduler.getJobProviderByIndex(indexName).getJobType();
                                    }
                                } catch (Exception e) {
                                    // Fallback to unknown if job provider not found
                                }*/

                                // Add job details
                                jobDetails.put("job_type", jobType);
                                jobDetails.put("job_id", jobId);
                                jobDetails.put("index_name", indexName);

                                // Add job parameter details
                                if (jobInfo.getJobParameter() != null) {
                                    jobDetails.put("name", jobInfo.getJobParameter().getName());
                                    jobDetails.put("enabled", jobInfo.getJobParameter().isEnabled());
                                    jobDetails.put("enabled_time", jobInfo.getJobParameter().getEnabledTime().toString());
                                    jobDetails.put("last_update_time", jobInfo.getJobParameter().getLastUpdateTime().toString());

                                    // Add schedule information
                                    if (jobInfo.getJobParameter().getSchedule() != null) {
                                        Map<String, Object> scheduleMap = new HashMap<>();

                                        // Set schedule type
                                        if (jobInfo.getJobParameter().getSchedule() instanceof IntervalSchedule) {
                                            scheduleMap.put("type", "interval");
                                            IntervalSchedule intervalSchedule = (IntervalSchedule) jobInfo.getJobParameter().getSchedule();
                                            scheduleMap.put("start_time", intervalSchedule.getStartTime().toString());
                                            scheduleMap.put("interval", intervalSchedule.getInterval());
                                            scheduleMap.put("unit", intervalSchedule.getUnit().toString());
                                        } else if (jobInfo.getJobParameter().getSchedule() instanceof CronSchedule) {
                                            scheduleMap.put("type", "cron");
                                            CronSchedule cronSchedule = (CronSchedule) jobInfo.getJobParameter().getSchedule();
                                            scheduleMap.put("expression", cronSchedule.getCronExpression());
                                            scheduleMap.put("timezone", cronSchedule.getTimeZone().getId());
                                        } else {
                                            scheduleMap.put("type", "unknown");
                                        }

                                        jobDetails.put("schedule", scheduleMap);

                                        // Add delay information
                                        jobDetails.put(
                                            "delay",
                                            jobInfo.getJobParameter().getSchedule().getDelay() != null
                                                ? jobInfo.getJobParameter().getSchedule().getDelay()
                                                : "none"
                                        );

                                        try {
                                            // Add next execution time
                                            if (jobInfo.getExpectedExecutionTime() != null) {
                                                jobDetails.put(
                                                    "next_expected_execution_time",
                                                    jobInfo.getExpectedExecutionTime().toString()
                                                );
                                            }

                                            // Add next time to execute
                                            java.time.Instant now = java.time.Instant.now();
                                            jobDetails.put(
                                                "next_time_to_execute",
                                                (now.plus(jobInfo.getJobParameter().getSchedule().nextTimeToExecute()).toString())
                                            );
                                        } catch (Exception e) {
                                            // Skip time calculations if they fail
                                        }
                                    }

                                    // Add jitter and lock duration
                                    /*jobDetails.put(
                                        "jitter",
                                        jobInfo.getJobParameter().getJitter() != null ? jobInfo.getJobParameter().getJitter() : "none"
                                    );
                                    jobDetails.put(
                                        "lock_duration",
                                        jobInfo.getJobParameter().getLockDurationSeconds() != null
                                            ? jobInfo.getJobParameter().getLockDurationSeconds()
                                            : "no_lock"
                                    );*/
                                }

                                // Add execution information
                                jobDetails.put("descheduled", jobInfo.isDescheduled());
                                if (jobInfo.getActualPreviousExecutionTime() != null) {
                                    jobDetails.put("last_execution_time", jobInfo.getActualPreviousExecutionTime());
                                }
                                if (jobInfo.getExpectedPreviousExecutionTime() != null) {
                                    jobDetails.put("last_expected_execution_time", jobInfo.getExpectedPreviousExecutionTime());
                                }

                                jobs.add(jobDetails);
                            }
                        }
                    }
                }
            }

            // Add jobs list and total count
            scheduledJobInfo.put("jobs", jobs);
            scheduledJobInfo.put("total_jobs", jobs.size());
        } catch (Exception e) {
            // If any exception occurs, return an empty jobs list
            scheduledJobInfo.put("jobs", new java.util.ArrayList<>());
            scheduledJobInfo.put("total_jobs", 0);
            scheduledJobInfo.put("error", e.getMessage());
        }

        response.setScheduledJobInfo(scheduledJobInfo);
        return response;
    }
}
