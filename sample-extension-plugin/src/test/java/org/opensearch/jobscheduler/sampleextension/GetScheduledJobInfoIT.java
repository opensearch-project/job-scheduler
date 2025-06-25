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

    @Before
    public void setupJobs() throws IOException, InterruptedException {
        SampleJobParameter jobParam1 = new SampleJobParameter(
            "test-job-1",
            "Test Job 1",
            "test-index-1",
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            30L,
            0.1
        );

        SampleJobParameter jobParam2 = new SampleJobParameter(
            "test-job-2",
            "Test Job 2",
            "test-index-2",
            new IntervalSchedule(Instant.now(), 10, ChronoUnit.MINUTES),
            60L,
            0.2
        );

        SampleJobParameter jobParam3 = new SampleJobParameter(
            "test-job-3",
            "Test Job 3",
            "test-index-3",
            new CronSchedule("30 2 * * *", ZoneId.of("America/New_York")),
            90L,
            0.3
        );

        SampleJobParameter jobParam4 = new SampleJobParameter(
            "test-job-4",
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

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseJson);
        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Verify all test jobs are present
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) responseJson.get("jobs");
        assertNotNull("Jobs list should not be null", jobs);
        assertEquals("Should have 4 jobs in the list", 4, jobs.size());

        // Check that all expected job IDs are present and validate job fields
        Set<String> expectedJobIds = Set.of("test-job-1", "test-job-2", "test-job-3", "test-job-4");
        Set<String> actualJobIds = new HashSet<>();
        for (Map<String, Object> job : jobs) {
            actualJobIds.add((String) job.get("job_id"));

            // Validate required fields are present
            assertEquals("job_type should be scheduler_sample_extension", "scheduler_sample_extension", job.get("job_type"));
            assertNotNull("job_id should not be null", job.get("job_id"));
            assertEquals("index_name should not be .scheduler_sample_extension", ".scheduler_sample_extension", job.get("index_name"));
            assertNotNull("name should not be null", job.get("name"));
            assertFalse("descheduled should be False", (Boolean) job.get("descheduled"));
            assertTrue("enabled should be True", (Boolean) job.get("enabled"));
            assertNotNull("enabled_time should not be null", job.get("enabled_time"));
            assertNotNull("last_update_time should not be null", job.get("last_update_time"));
            assertNotNull("schedule should not be null", job.get("schedule"));

            // Validate schedule object
            @SuppressWarnings("unchecked")
            Map<String, Object> schedule = (Map<String, Object>) job.get("schedule");
            assertTrue(
                "schedule should be interval or Cron",
                ((schedule.get("type").equals("interval")) || (schedule.get("type").equals("cron")))
            );
        }
        assertEquals("All expected job IDs should be present", expectedJobIds, actualJobIds);
    }

    public void testGetScheduledJobInfoByNode() throws IOException {

        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/api/jobs?by_node", Collections.emptyMap(), null);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseJson);
        assertTrue("Response should contain scheduled job information", responseJson.containsKey("nodes"));
        assertEquals("Should have 4 total jobs", 4, responseJson.get("total_jobs"));

        // Verify nodes array contains job information
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) responseJson.get("nodes");
        assertNotNull("Nodes list should not be null", nodes);
        assertFalse("Should have at least one node", nodes.isEmpty());

        // Collect all job IDs across all nodes
        Set<String> allJobIds = new HashSet<>();
        for (Map<String, Object> node : nodes) {

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodeJobs = (List<Map<String, Object>>) ((Map<String, Object>) node.get("scheduled_job_info")).get(
                "jobs"
            );
            if (nodeJobs != null) {
                for (Map<String, Object> job : nodeJobs) {
                    allJobIds.add((String) job.get("job_id"));
                    assertEquals("job_type should be scheduler_sample_extension", "scheduler_sample_extension", job.get("job_type"));
                    assertNotNull("job_id should not be null", job.get("job_id"));
                    assertEquals(
                        "index_name should not be .scheduler_sample_extension",
                        ".scheduler_sample_extension",
                        job.get("index_name")
                    );
                    assertNotNull("name should not be null", job.get("name"));
                    assertFalse("descheduled should be False", (Boolean) job.get("descheduled"));
                    assertTrue("enabled should be True", (Boolean) job.get("enabled"));
                    assertNotNull("enabled_time should not be null", job.get("enabled_time"));
                    assertNotNull("last_update_time should not be null", job.get("last_update_time"));
                    assertNotNull("schedule should not be null", job.get("schedule"));

                    // Validate schedule object
                    @SuppressWarnings("unchecked")
                    Map<String, Object> schedule = (Map<String, Object>) job.get("schedule");
                    assertTrue(
                        "schedule should be interval or Cron",
                        ((schedule.get("type").equals("interval")) || (schedule.get("type").equals("cron")))
                    );
                }
            }
        }

        java.util.Set<String> expectedJobIds = java.util.Set.of("test-job-1", "test-job-2", "test-job-3", "test-job-4");
        assertEquals("All expected job IDs should be present across nodes", expectedJobIds, allJobIds);

        // Validate job fields across all nodes

    }

    @After
    public void cleanupJobs() throws IOException {
        deleteWatcherJob("test-job-1");
        deleteWatcherJob("test-job-2");
        deleteWatcherJob("test-job-3");
        deleteWatcherJob("test-job-4");
    }
}
