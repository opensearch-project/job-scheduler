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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.rest.request.GetSchedulingInfoRequest;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * This class consists of the REST handler to GET job details from extensions.
 */
public class RestGetSchedulingInfoAction extends BaseRestHandler {

    public static final String GET_ALL_JOB_INFO_ACTION = "get_all_job_info_action";

    private final Logger logger = LogManager.getLogger(RestGetSchedulingInfoAction.class);

    private final JobDetailsService jobDetailsService;


    public RestGetSchedulingInfoAction(final JobDetailsService jobDetailsService) {
        this.jobDetailsService = jobDetailsService;
    }

    @Override
    public String getName() { return GET_ALL_JOB_INFO_ACTION; }

    @Override
    public List<Route> routes() {
        return List.of(
            // Get All Job Info Request
            new Route(GET, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_jobs"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        GetSchedulingInfoRequest getSchedulingInfoRequest = GetSchedulingInfoRequest.parse(parser);
        boolean activeJobsOnly = getSchedulingInfoRequest.isActiveJobsOnly();

        CompletableFuture<Map<String, JobDetails>> inProgressFuture = new CompletableFuture<>();
        
        Map<String, JobDetails> allJobDetails = JobDetailsService.getIndexToJobDetails();
        
        // Filter jobs if activeJobsOnly is true
        inProgressFuture.complete(allJobDetails);

        try {
            inProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Get All Job Info timed out ", e);
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }

        return channel -> {
            Map<String, JobDetails> jobDetailsMap = null;
            try {
                jobDetailsMap = inProgressFuture.get();
            } catch (Exception e) {
                logger.error("Exception occurred in get all job info ", e);
            }

            XContentBuilder builder = channel.newBuilder();
            RestStatus restStatus = RestStatus.OK;
            BytesRestResponse bytesRestResponse;
            
            try {
                builder.startObject();
                builder.field("total_jobs", jobDetailsMap != null ? jobDetailsMap.size() : 0);
                
                if (jobDetailsMap != null && !jobDetailsMap.isEmpty()) {
                    builder.startArray("jobs");
                    for (Map.Entry<String, JobDetails> entry : jobDetailsMap.entrySet()) {
                        builder.startObject();
                        builder.field("id", entry.getKey());
                        JobDetails jobDetails = entry.getValue();
                        builder.field("job_index", jobDetails.getJobIndex());
                        builder.field("job_type", jobDetails.getJobType());
                        builder.field("extension_unique_id", jobDetails.getExtensionUniqueId());
                        builder.endObject();
                    }
                    builder.endArray();
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