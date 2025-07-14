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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;
import org.opensearch.jobscheduler.scheduler.ScheduledJobInfo;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoNodeRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoNodeResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class TransportGetScheduledInfoAction extends TransportNodesAction<
    GetScheduledInfoRequest,
    GetScheduledInfoResponse,
    GetScheduledInfoNodeRequest,
    GetScheduledInfoNodeResponse> {

    private static final Logger log = LogManager.getLogger(JobScheduler.class);
    private final JobScheduler jobScheduler;
    private final JobDetailsService jobDetailsService;
    private static final DateFormatter STRICT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time");
    private final Client client;

    @Inject
    public TransportGetScheduledInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        JobScheduler jobScheduler,
        JobDetailsService jobDetailsService,
        Client client
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
        this.jobDetailsService = jobDetailsService;
        this.client = client;
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

    private List<Map<String, Object>> findLockByJobId(String jobId) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            SearchRequest searchRequest = new SearchRequest(".opendistro-job-scheduler-lock");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("job_id", jobId));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest).actionGet();
            List<Map<String, Object>> lock = new ArrayList<>();
            searchResponse.getHits().forEach(hit -> lock.add(hit.getSourceAsMap()));

            if (lock.get(0).containsKey("lock_time")) {
                Object lockTime = lock.get(0).get("lock_time");
                if (lockTime instanceof Number) {
                    long lockTimeSeconds = ((Number) lockTime).longValue();
                    String formattedLockTime = STRICT_DATE_TIME_FORMATTER.format(
                        Instant.ofEpochSecond(lockTimeSeconds).atOffset(ZoneOffset.UTC)
                    );
                    lock.get(0).put("lock_time", formattedLockTime);
                }
            }

            return lock;
        } catch (Exception e) {
            log.debug("Error in find Locks by Job id {}", jobId, e);
            return new ArrayList<>();
        }
    }

    @Override
    protected GetScheduledInfoNodeResponse nodeOperation(GetScheduledInfoNodeRequest request) {
        GetScheduledInfoNodeResponse response = new GetScheduledInfoNodeResponse(clusterService.localNode());
        Map<String, Object> scheduledJobInfo = new HashMap<>();
        Map<String, ScheduledJobProvider> indexToJobProvider = jobDetailsService.getIndexToJobProviders();

        try {
            // Create a list to hold all job details
            List<Map<String, Object>> jobs = new ArrayList<>();

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

                                if (jobInfo == null) {
                                    log.debug("JobInfo {} does not exist.", jobId);
                                    continue;
                                }

                                Map<String, Object> jobDetails = new LinkedHashMap<>();

                                String jobType = indexToJobProvider.get(indexName).getJobType();

                                // Add job details
                                jobDetails.put("job_type", jobType);
                                jobDetails.put("job_id", jobId);
                                jobDetails.put("index_name", indexName);

                                // Add job parameter details
                                jobDetails.put("name", jobInfo.getJobParameter().getName());
                                jobDetails.put("descheduled", jobInfo.isDescheduled());
                                jobDetails.put("enabled", jobInfo.getJobParameter().isEnabled());
                                jobDetails.put(
                                    "enabled_time",
                                    STRICT_DATE_TIME_FORMATTER.format(jobInfo.getJobParameter().getEnabledTime().atOffset(ZoneOffset.UTC))
                                );
                                jobDetails.put(
                                    "last_update_time",
                                    STRICT_DATE_TIME_FORMATTER.format(
                                        jobInfo.getJobParameter().getLastUpdateTime().atOffset(ZoneOffset.UTC)
                                    )
                                );

                                // Add execution information
                                if (jobInfo.getActualPreviousExecutionTime() != null) {
                                    jobDetails.put(
                                        "last_execution_time",
                                        STRICT_DATE_TIME_FORMATTER.format(jobInfo.getActualPreviousExecutionTime().atOffset(ZoneOffset.UTC))
                                    );
                                } else {
                                    jobDetails.put("last_execution_time", "none");
                                }
                                if (jobInfo.getExpectedPreviousExecutionTime() != null) {
                                    jobDetails.put(
                                        "last_expected_execution_time",
                                        STRICT_DATE_TIME_FORMATTER.format(
                                            jobInfo.getExpectedPreviousExecutionTime().atOffset(ZoneOffset.UTC)
                                        )
                                    );
                                } else {
                                    jobDetails.put("last_expected_execution_time", "none");
                                }

                                // Add next execution time
                                if (jobInfo.getExpectedExecutionTime() != null) {
                                    jobDetails.put(
                                        "next_expected_execution_time",
                                        STRICT_DATE_TIME_FORMATTER.format(jobInfo.getExpectedExecutionTime().atOffset(ZoneOffset.UTC))
                                    );
                                } else {
                                    jobDetails.put("next_expected_execution_time", "none");
                                }

                                // Add schedule information
                                if (jobInfo.getJobParameter().getSchedule() == null) {
                                    log.debug("Schedule for job {} does not exist.", jobId);
                                } else {
                                    Map<String, Object> scheduleMap = new LinkedHashMap<>();

                                    // Set schedule type
                                    if (jobInfo.getJobParameter().getSchedule() instanceof IntervalSchedule intervalSchedule) {
                                        scheduleMap.put("type", IntervalSchedule.INTERVAL_FIELD);
                                        scheduleMap.put(
                                            "start_time",
                                            STRICT_DATE_TIME_FORMATTER.format(intervalSchedule.getStartTime().atOffset(ZoneOffset.UTC))
                                        );
                                        scheduleMap.put("interval", intervalSchedule.getInterval());
                                        scheduleMap.put("unit", intervalSchedule.getUnit().toString());
                                        scheduleMap.put(
                                            "delay",
                                            jobInfo.getJobParameter().getSchedule().getDelay() != null
                                                ? jobInfo.getJobParameter().getSchedule().getDelay()
                                                : "none"
                                        );
                                    } else if (jobInfo.getJobParameter().getSchedule() instanceof CronSchedule cronSchedule) {
                                        scheduleMap.put("type", CronSchedule.CRON_FIELD);
                                        scheduleMap.put("expression", cronSchedule.getCronExpression());
                                        scheduleMap.put("timezone", cronSchedule.getTimeZone().getId());
                                        scheduleMap.put(
                                            "delay",
                                            jobInfo.getJobParameter().getSchedule().getDelay() != null
                                                ? jobInfo.getJobParameter().getSchedule().getDelay()
                                                : "none"
                                        );
                                    } else {
                                        scheduleMap.put("type", "unknown");
                                    }

                                    jobDetails.put("schedule", scheduleMap);
                                }

                                // Add lock information
                                jobDetails.put("lock", findLockByJobId(jobId));

                                // Add jitter and lock duration
                                jobDetails.put(
                                    "jitter",
                                    jobInfo.getJobParameter().getJitter() != null ? jobInfo.getJobParameter().getJitter() : "none"
                                );
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
