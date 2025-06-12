/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration test for RestGetSchedulingInfoAction
 */
public class RestGetSchedulingInfoActionIT extends OpenSearchRestTestCase {

    private static final String SCHEDULER_INFO_URI = "/_plugins/_job_scheduler/_job_scheduling_info";

    private ThreadPool threadPool;

    private JobScheduler scheduler;

    private JobDocVersion dummyVersion = new JobDocVersion(1L, 1L, 1L);
    private Double jitterLimit = 0.95;

    public void testGetSchedulingInfoEndpoint() throws IOException {
        Request schedulingInfoRequest = new Request("GET", SCHEDULER_INFO_URI);
        Response schedulingInfoResponse = client().performRequest(schedulingInfoRequest);

        assertEquals(RestStatus.OK.getStatus(), schedulingInfoResponse.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), null, schedulingInfoResponse.getEntity().getContent())
            .map();

        // Verify response contains expected fields
        assertThat(responseMap.get("jobs"), notNullValue());
        assertThat(responseMap.get("total_jobs"), notNullValue());
        assertTrue(responseMap.get("jobs") instanceof java.util.List);
        assertTrue(responseMap.get("total_jobs") instanceof Integer);
    }

    public void testGetSchedulingInfoStructure() throws IOException {
        Request schedulingInfoRequest = new Request("GET", SCHEDULER_INFO_URI);
        Response schedulingInfoResponse = client().performRequest(schedulingInfoRequest);

        Map<String, Object> responseMap = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), null, schedulingInfoResponse.getEntity().getContent())
            .map();

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> jobs = (java.util.List<Map<String, Object>>) responseMap.get("jobs");

        if (!jobs.isEmpty()) {
            Map<String, Object> job = jobs.get(0);

            assertThat(job.get("job_id"), notNullValue());
            assertThat(job.get("index_name"), notNullValue());
            assertThat(job.get("name"), notNullValue());
            assertThat(job.get("enabled"), notNullValue());
            assertThat(job.get("schedule"), notNullValue());
            assertThat(job.get("descheduled"), notNullValue());
        }

        // Verify total_jobs matches the actual number of jobs
        assertThat(responseMap.get("total_jobs"), equalTo(jobs.size()));
    }
}
