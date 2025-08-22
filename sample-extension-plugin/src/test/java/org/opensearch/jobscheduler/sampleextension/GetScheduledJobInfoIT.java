/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.junit.Before;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

public class GetScheduledJobInfoIT extends SampleExtensionIntegTestCase {

    Set<String> expectedJobIds = Set.of("test-job-1", "test-job-2", "test-job-3", "test-job-4");

    private SampleJobParameter jobParam1;
    private SampleJobParameter jobParam2;
    private SampleJobParameter jobParam3;
    private SampleJobParameter jobParam4;

    @Before
    public void setupJobs() throws IOException, InterruptedException {
        jobParam1 = new SampleJobParameter(
            "Test Job 1",
            "test-index-1",
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            30L,
            0.1
        );

        jobParam2 = new SampleJobParameter(
            "Test Job 2",
            "test-index-2",
            new IntervalSchedule(Instant.now(), 10, ChronoUnit.MINUTES),
            60L,
            0.2
        );

        jobParam3 = new SampleJobParameter(
            "Test Job 3",
            "test-index-3",
            new CronSchedule("30 2 * * *", ZoneId.of("America/New_York")),
            90L,
            0.3
        );

        jobParam4 = new SampleJobParameter(
            "Test Job 4",
            "test-index-4",
            new CronSchedule("0 9 * * MON", ZoneId.systemDefault()),
            120L,
            0.4
        );

        createWatcherJob("test-job-1", jobParam1);
        createWatcherJob("test-job-2", jobParam2);
        createWatcherJob("test-job-3", jobParam3);
        createWatcherJob("test-job-4", jobParam4);
        // Refresh indices to ensure all jobs are available
        makeRequest(client(), "POST", "/_refresh", Collections.emptyMap(), null);

    }

    public void testGetScheduledJobInfoEntireCluster() throws IOException {

        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        Map<String, Object> responseJson = parseResponse(response);

        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Verify all test jobs are present
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) responseJson.get("jobs");
        assertNotNull("Jobs list should not be null", jobs);
        assertEquals("Should have 4 jobs in the list", 4, jobs.size());

        // Check that all expected job IDs are present and validate job fields
        Set<String> actualJobIds = new HashSet<>();
        for (Map<String, Object> job : jobs) {
            validateJobFields(job, true, false, actualJobIds);
        }
        assertEquals("All expected job IDs should be present", expectedJobIds, actualJobIds);
    }

    public void testGetScheduledJobInfoByNode() throws IOException {

        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs?by_node", Collections.emptyMap(), null);

        Map<String, Object> responseJson = parseResponse(response);

        assertTrue("Response should contain scheduled job information", responseJson.containsKey("nodes"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Verify nodes array contains job information
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) responseJson.get("nodes");
        assertNotNull("Nodes list should not be null", nodes);
        assertFalse("Should have at least one node", nodes.isEmpty());

        // Collect all job IDs across all nodes
        Set<String> actualJobIds = new HashSet<>();
        for (Map<String, Object> node : nodes) {

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodeJobs = (List<Map<String, Object>>) node.get("jobs");
            if (nodeJobs != null) {
                for (Map<String, Object> job : nodeJobs) {
                    validateJobFields(job, true, false, actualJobIds);
                }
            }
        }

        assertEquals("All expected job IDs should be present across nodes", expectedJobIds, actualJobIds);
    }

    public void testDeScheduledJobInfo() throws IOException {

        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        Map<String, Object> responseJson = parseResponse(response);

        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        disableWatcherJob("test-job-1", jobParam1);
        disableWatcherJob("test-job-2", jobParam2);
        disableWatcherJob("test-job-3", jobParam3);
        disableWatcherJob("test-job-4", jobParam4);

        response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        responseJson = parseResponse(response);

        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Verify all disabled test jobs are present
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) responseJson.get("jobs");
        assertNotNull("Jobs list should not be null", jobs);
        assertEquals("Should have 4 jobs in the list", 4, jobs.size());

        // Check that all expected job IDs are present and validate job fields
        Set<String> actualJobIds = new HashSet<>();
        for (Map<String, Object> job : jobs) {
            validateJobFields(job, false, true, actualJobIds);
        }
        assertEquals("All expected job IDs should be present", expectedJobIds, actualJobIds);
    }

    public void testDeleteDeScheduledJobInfo() throws IOException {
        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        Map<String, Object> responseJson = parseResponse(response);

        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Only disables jobs 1 and 2
        disableWatcherJob("test-job-1", jobParam1);
        disableWatcherJob("test-job-2", jobParam2);

        response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        responseJson = parseResponse(response);

        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Deletes and removes a scheduled job and a DeScheduled job
        deleteWatcherJob("test-job-1");
        deleteWatcherJob("test-job-3");

        response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        responseJson = parseResponse(response);

        // Ensures that both deleted jobs are removed from the Scheduled/DeScheduled list and other jobs persist
        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 2 total jobs", 2, responseJson.get("total_jobs"));
    }

    public void testDisableThenEnableJobInfo() throws IOException {
        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);

        Map<String, Object> responseJson = parseResponse(response);

        // verify all jobs are enabled and scheduled
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) responseJson.get("jobs");
        Set<String> actualJobIds = new HashSet<>();
        for (Map<String, Object> job : jobs) {
            validateJobFields(job, true, false, actualJobIds);
        }
        assertEquals("All expected job IDs should be present", expectedJobIds, actualJobIds);
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        disableWatcherJob("test-job-1", jobParam1);
        disableWatcherJob("test-job-2", jobParam2);
        disableWatcherJob("test-job-3", jobParam3);
        disableWatcherJob("test-job-4", jobParam4);

        response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);
        responseJson = parseResponse(response);

        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Verify all jobs are disabled and descheduled
        jobs = (List<Map<String, Object>>) responseJson.get("jobs");
        assertNotNull("Jobs list should not be null", jobs);
        assertEquals("Should have 4 jobs in the list", 4, jobs.size());

        // Check that all expected job IDs are present and validate job fields
        actualJobIds = new HashSet<>();
        for (Map<String, Object> job : jobs) {
            validateJobFields(job, false, true, actualJobIds);
        }
        assertEquals("All expected job IDs should be present", expectedJobIds, actualJobIds);

        // verify all jobs are enabled and scheduled without duplication
        enableWatcherJob("test-job-1", jobParam1);
        enableWatcherJob("test-job-2", jobParam2);
        enableWatcherJob("test-job-3", jobParam3);
        enableWatcherJob("test-job-4", jobParam4);

        response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs", Collections.emptyMap(), null);
        responseJson = parseResponse(response);

        jobs = (List<Map<String, Object>>) responseJson.get("jobs");
        assertNotNull("Jobs list should not be null", jobs);
        assertEquals("Should have 4 jobs in the list", 4, jobs.size());

        actualJobIds = new HashSet<>();
        for (Map<String, Object> job : jobs) {
            validateJobFields(job, true, false, actualJobIds);
        }
        assertEquals("All expected job IDs should be present", expectedJobIds, actualJobIds);
    }

    @SuppressWarnings("unchecked")
    private void validateJobFields(
        Map<String, Object> job,
        boolean expectedEnabled,
        boolean expectedDescheduled,
        Set<String> actualJobIds
    ) {
        assertEquals("scheduler_sample_extension", job.get("job_type"));
        assertNotNull(job.get("job_id"));
        actualJobIds.add((String) job.get("job_id"));
        assertEquals(".scheduler_sample_extension", job.get("index_name"));
        assertEquals(expectedDescheduled, job.get("descheduled"));
        Map<String, Object> jobParameter = (Map<String, Object>) job.get("job_parameter");
        assertNotNull(jobParameter.get("name"));
        assertEquals(expectedEnabled, jobParameter.get("enabled"));
        assertNotNull(jobParameter.get("enabled_time"));
        assertNotNull(jobParameter.get("last_update_time"));
        assertNotNull(jobParameter.get("schedule"));
        assertNull(jobParameter.get("jitter"));

        Map<String, Object> schedule = (Map<String, Object>) jobParameter.get("schedule");
        assertTrue(schedule.containsKey("interval") || schedule.containsKey("cron"));
    }

    private Map<String, Object> parseResponse(Response response) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
    }

    @After
    public void cleanupJobs() throws IOException {
        deleteWatcherJob("test-job-1");
        deleteWatcherJob("test-job-2");
        deleteWatcherJob("test-job-3");
        deleteWatcherJob("test-job-4");
    }
}
