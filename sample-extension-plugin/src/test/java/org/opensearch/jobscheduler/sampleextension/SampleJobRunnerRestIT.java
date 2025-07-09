/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.junit.Assert;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class SampleJobRunnerRestIT extends SampleExtensionIntegTestCase {

    public void testJobCreateWithCorrectParams() throws IOException {
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch("http-logs");
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        // Asserts that job is created with correct parameters.
        Assert.assertEquals(jobParameter.getName(), schedJobParameter.getName());
        Assert.assertEquals(jobParameter.getIndexToWatch(), schedJobParameter.getIndexToWatch());
        Assert.assertEquals(jobParameter.getLockDurationSeconds(), schedJobParameter.getLockDurationSeconds());
    }

    public void testJobDeleteWithDescheduleJob() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        // wait till the job runner runs for the first time after 1 min & inserts a record into the watched index & then delete the job.
        waitAndDeleteWatcherJob(schedJobParameter.getIndexToWatch(), jobId);
        long actualCount = waitAndCountRecords(index, 130000);

        // Asserts that in the last 3 mins, no new job ran to insert a record into the watched index & all locks are deleted for the job.
        Assert.assertEquals(1, actualCount);
        Assert.assertEquals(0L, getLockTimeByJobId(jobId));
    }

    public void testJobUpdateWithRescheduleJob() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        // update the job params to now watch a new index.
        String newIndex = createTestIndex();
        jobParameter.setIndexToWatch(newIndex);

        // wait till the job runner runs for the first time after 1 min & inserts a record into the watched index & then update the job with
        // new params.
        waitAndCreateWatcherJob(schedJobParameter.getIndexToWatch(), jobId, jobParameter);
        long actualCount = waitAndCountRecords(newIndex, 130000);

        // Asserts that the job runner has the updated params & it inserted the record in the new watched index.
        Assert.assertEquals(1, actualCount);
        long prevIndexActualCount = waitAndCountRecords(index, 0);

        // Asserts that the job runner no longer updates the old index as the job params have been updated.
        Assert.assertEquals(1, prevIndexActualCount);
    }

    public void testJobUpdateWithRescheduleJobThenListJobs() throws Exception {

        String SCHEDULER_INFO_URI = "/_plugins/_job_scheduler/api/jobs?by_node";

        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        for (int i = 0; i < 10; i++) {
            // Creates a new watcher job.
            String indexN = createTestIndex();
            SampleJobParameter jobParameterN = new SampleJobParameter();
            jobParameterN.setJobName("sample-job-it" + i);
            jobParameterN.setIndexToWatch(indexN);
            jobParameterN.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
            jobParameterN.setLockDurationSeconds(120L);

            String jobIdN = OpenSearchRestTestCase.randomAlphaOfLength(10);
            createWatcherJob(jobIdN, jobParameterN);
        }

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        // update the job params to now watch a new index.
        String newIndex = createTestIndex();
        jobParameter.setIndexToWatch(newIndex);

        // wait till the job runner runs for the first time after 1 min & inserts a record into the watched index & then update the job with
        // new params.
        waitAndCreateWatcherJob(schedJobParameter.getIndexToWatch(), jobId, jobParameter);
        long actualCount = waitAndCountRecords(newIndex, 130000);

        // Asserts that the job runner has the updated params & it inserted the record in the new watched index.
        Assert.assertEquals(1, actualCount);
        long prevIndexActualCount = waitAndCountRecords(index, 0);

        // Asserts that the job runner no longer updates the old index as the job params have been updated.
        Assert.assertEquals(1, prevIndexActualCount);

        Response response = makeRequest(client(), "GET", SCHEDULER_INFO_URI, Map.of(), null);
        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();

        List<Map<String, Object>> nodes = (List<Map<String, Object>>) responseJson.get("nodes");
        assertNotNull("Nodes list should not be null", nodes);
        assertEquals(11, responseJson.get("total_jobs"));
        assertEquals(0, ((List<?>) responseJson.get("failures")).size());
        assertFalse("Should have at least one node", nodes.isEmpty());

        for (Map<String, Object> node : nodes) {

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodeJobs = (List<Map<String, Object>>) ((Map<String, Object>) node.get("scheduled_job_info")).get(
                "jobs"
            );
            if (nodeJobs != null) {
                for (Map<String, Object> job : nodeJobs) {
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
                    assertTrue(job.get("lock_duration") instanceof Integer);
                    assertEquals("none", job.get("jitter"));
                    assertEquals("none", job.get("delay"));
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
    }

    public void testAcquiredLockPreventExecOfTasks() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        // ensures that the next job tries to run even before the previous job finished & released its lock. Also look at
        // SampleJobRunner.runTaskForLockIntegrationTests
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        // Asserts that the job runner is running for the first time & it has inserted a new record into the watched index.
        long actualCount = waitAndCountRecords(index, 80000);
        Assert.assertEquals(1, actualCount);

        // gets the lock time for the lock acquired for running first job.
        long lockTime = getLockTimeByJobId(jobId);

        // Asserts that the second job could not run & hence no new record is inserted into the watched index.
        // Also asserts that the old lock acquired for running first job is still not released.
        actualCount = waitAndCountRecords(index, 80000);
        Assert.assertEquals(1, actualCount);
        Assert.assertTrue(doesLockExistByLockTime(lockTime));

        // Asserts that the new job ran after 2 mins after the first job lock is released. Hence new record is inserted into the watched
        // index.
        // Also asserts that the old lock is released.
        actualCount = waitAndCountRecords(index, 130000);
        Assert.assertEquals(2, actualCount);
        Assert.assertFalse(doesLockExistByLockTime(lockTime));
    }
}
