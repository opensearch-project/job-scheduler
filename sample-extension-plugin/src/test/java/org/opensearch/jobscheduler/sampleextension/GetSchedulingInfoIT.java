/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Integration test for GetSchedulingInfo API
 */
public class GetSchedulingInfoIT extends SampleExtensionIntegTestCase {

    private static final String SCHEDULER_INFO_URI = "/_plugins/_job_scheduler/_job_scheduling_info";
    private String testIndex;
    private String jobId1;
    private String jobId2;
    private String jobId3;
    private Set<String> jobID;

    @Before
    public void setupTest() throws Exception {
        // Create test index
        testIndex = ".scheduler_sample_extension";
        // Create a job to ensure there's at least one job in the scheduler
        jobId1 = "test-job-1-" + randomAlphaOfLength(5);

        SampleJobParameter jobParameter1 = new SampleJobParameter(
            jobId1,
            "Test Job",
            testIndex,
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            60L,
            0.5
        );

        createWatcherJob(jobId1, jobParameter1);
        // Wait for job to be scheduled
        Thread.sleep(1000);

        jobId2 = "test-job-2-" + randomAlphaOfLength(5);
        SampleJobParameter jobParameter2 = new SampleJobParameter(
            jobId2,
            "Test Job",
            testIndex,
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            60L,
            0.5
        );

        createWatcherJob(jobId2, jobParameter2);
        // Wait for job to be scheduled
        Thread.sleep(1000);

        jobId3 = "test-job-3-" + randomAlphaOfLength(5);
        SampleJobParameter jobParameter3 = new SampleJobParameter(
            jobId3,
            "Test Job",
            testIndex,
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            60L,
            0.5
        );

        createWatcherJob(jobId3, jobParameter3);
        // Wait for job to be scheduled
        Thread.sleep(1000);
        jobID = Set.of(jobId1, jobId2, jobId3);
    }

    @After
    public void cleanupTest() throws IOException {
        // Delete the job
        deleteWatcherJob(jobId1);
        deleteWatcherJob(jobId2);
        deleteWatcherJob(jobId3);

        // Delete test index
        deleteTestIndex(testIndex);
    }

    public void testGetSchedulingInfo() throws IOException {
        // Call the GetSchedulingInfo API
        Response response = makeRequest(client(), "GET", SCHEDULER_INFO_URI, Collections.emptyMap(), null);

        // Verify response status
        assertEquals("API should return OK status", RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

        // Parse response
        Map<String, Object> responseMap = parseResponseAsMap(response);

        // Verify response structure
        assertTrue("Response should contain jobs array", responseMap.containsKey("jobs"));
        assertTrue("Response should contain total_jobs count", responseMap.containsKey("total_jobs"));

        // Verify jobs array
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> jobs = (java.util.List<Map<String, Object>>) responseMap.get("jobs");
        assertNotNull("Jobs list should not be null", jobs);

        // Verify our job is in the list
        boolean foundJob = false;
        for (Map<String, Object> job : jobs) {

            if (jobID.contains(job.get("job_id"))) {
                foundJob = true;
                assertEquals("Test Job", job.get("name"));
                assertEquals(testIndex, job.get("index_name"));
                assertTrue((Boolean) job.get("enabled"));
                assertFalse((Boolean) job.get("descheduled"));
            }
        }

        assertTrue("Should find our test job in the scheduling info", foundJob);

        // Verify total_jobs count matches
        int totalJobs = Integer.parseInt(responseMap.get("total_jobs").toString());
        assertEquals(jobs.size(), totalJobs);
    }

    private Map<String, Object> parseResponseAsMap(Response response) throws IOException {
        return org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
    }
}
