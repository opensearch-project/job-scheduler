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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.scheduler.ScheduledJobInfo;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler to get all scheduled job information
 */
public class RestGetSchedulingInfoAction extends BaseRestHandler {

    public static final String GET_SCHEDULING_INFO_ACTION = "get_scheduling_info_action";
    private final Logger logger = LogManager.getLogger(RestGetSchedulingInfoAction.class);
    private final JobScheduler jobScheduler;
    private Map<String, ScheduledJobProvider> indexToJobProviders;

    public RestGetSchedulingInfoAction(final JobScheduler jobScheduler, Map<String, ScheduledJobProvider> indexToJobProviders) {
        this.jobScheduler = jobScheduler;
        this.indexToJobProviders = indexToJobProviders;
    }

    @Override
    public String getName() {
        return GET_SCHEDULING_INFO_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_job_scheduling_info")));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            BytesRestResponse bytesRestResponse;

            try {
                builder.startObject();

                ScheduledJobInfo scheduledJobInfo = jobScheduler.getScheduledJobInfo();

                int totalJobs = 0;

                builder.startArray("jobs");

                for (Map.Entry<String, Map<String, JobSchedulingInfo>> indexEntry : scheduledJobInfo.getJobInfoMap().entrySet()) {
                    String indexName = indexEntry.getKey();
                    String jobtype = indexToJobProviders.get(indexName).getJobType();
                    Map<String, JobSchedulingInfo> jobs = indexEntry.getValue();

                    for (Map.Entry<String, JobSchedulingInfo> jobEntry : jobs.entrySet()) {
                        String jobId = jobEntry.getKey();
                        JobSchedulingInfo jobInfo = jobEntry.getValue();
                        ScheduledJobParameter jobParameter = jobInfo.getJobParameter();

                        builder.startObject();
                        builder.field("job_type", jobtype);
                        builder.field("job_id", jobId);
                        builder.field("index_name", indexName);
                        builder.field("name", jobParameter.getName());
                        builder.field("enabled", jobParameter.isEnabled());
                        builder.field("enabled_time", jobParameter.getEnabledTime());
                        builder.field("descheduled", jobInfo.isDescheduled());
                        builder.field("last_update_time", jobParameter.getLastUpdateTime());

                        if (jobInfo.getActualPreviousExecutionTime() != null) {
                            builder.field("last_execution_time", jobInfo.getActualPreviousExecutionTime());
                        }
                        if (jobInfo.getExpectedPreviousExecutionTime() != null) {
                            builder.field("last_expected_execution_time", jobInfo.getExpectedPreviousExecutionTime());
                        }
                        if (jobInfo.getExpectedExecutionTime() != null) {
                            builder.field("next_expected_execution_time", jobInfo.getExpectedExecutionTime());
                        }
                        if (jobInfo.getActualPreviousExecutionTime() != null) {
                            builder.field("on_time", jobParameter.getSchedule().runningOnTime(jobInfo.getActualPreviousExecutionTime()));
                        }

                        Schedule schedule = jobParameter.getSchedule();
                        Instant now = Instant.now();
                        builder.field("next_time_to_execute", now.plus(schedule.nextTimeToExecute()));

                        if (schedule.getDelay() != null) {
                            builder.field("delay", schedule.getDelay());
                        } else {
                            builder.field("delay", "none");
                        }

                        builder.startObject("schedule");
                        if (schedule instanceof IntervalSchedule) {
                            IntervalSchedule intervalSchedule = (IntervalSchedule) schedule;
                            builder.field("type", "interval");
                            builder.field("start_time", intervalSchedule.getStartTime());
                            builder.field("interval", intervalSchedule.getInterval());
                            builder.field("unit", intervalSchedule.getUnit().toString());
                        } else if (schedule instanceof CronSchedule) {
                            CronSchedule cronSchedule = (CronSchedule) schedule;
                            builder.field("type", "cron");
                            builder.field("expression", cronSchedule.getCronExpression());
                            builder.field("timezone", cronSchedule.getTimeZone().getId());
                        } else {
                            builder.field("type", "unknown");
                            builder.field("raw_schedule", schedule.toString());
                        }
                        builder.endObject();

                        if (jobParameter.getJitter() != null) {
                            builder.field("jitter", jobParameter.getJitter());
                        } else {
                            builder.field("jitter", "none");
                        }

                        if (jobParameter.getLockDurationSeconds() != null) {
                            builder.field("lock_duration", jobParameter.getLockDurationSeconds());
                        } else {
                            builder.field("lock_duration", "no_lock");
                        }
                        builder.endObject();
                        totalJobs++;
                    }
                }

                builder.endArray();
                builder.field("total_jobs", totalJobs);
                builder.endObject();

                bytesRestResponse = new BytesRestResponse(restStatus, builder);
            } catch (Exception e) {
                logger.error("Failed to get scheduling info", e);
                builder.startObject();
                builder.field("error", e.getMessage());
                builder.endObject();
                bytesRestResponse = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
            } finally {
                builder.close();
            }

            channel.sendResponse(bytesRestResponse);
        };
    }
}
