/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hc.core5.http.io.entity.EntityUtils;

public class SampleJobRunnerRestIT extends SampleExtensionIntegTestCase {

    public static final String LOCK_INFO_URI = "/_plugins/_job_scheduler/api/locks";
    public static final String SCHEDULER_INFO_URI = "/_plugins/_job_scheduler/api/jobs?by_node";
    public static final String SCHEDULER_INFO_URI_CLUSTER = "/_plugins/_job_scheduler/api/jobs";
    public static final String HISTORY_INFO_URI = "/_plugins/_job_scheduler/api/history";

    public void testJobCreateWithCorrectParams() throws IOException {
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch("http-logs");
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(5L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        // Asserts that job is created with correct parameters.
        Assert.assertEquals(jobParameter.getName(), schedJobParameter.getName());
        Assert.assertEquals(jobParameter.getIndexToWatch(), schedJobParameter.getIndexToWatch());
        Assert.assertEquals(jobParameter.getLockDurationSeconds(), schedJobParameter.getLockDurationSeconds());

        // Cleanup
        deleteWatcherJob(jobId);
    }

    public void testJobDeleteWithDescheduleJob() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(5L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        waitUntilLockIsAcquiredAndReleased(jobId);

        // wait till the job runner runs for the first time after 5s & inserts a record into the watched index & then delete the job.
        deleteWatcherJob(jobId);

        // ensure log remains released as job is now descheduled
        assertThrows(
            ConditionTimeoutException.class,
            () -> await().atMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
                LockModel lock = getLockByJobId(jobId);
                if (lock != null && !lock.isReleased()) {
                    Assert.fail("Lock should not be acquired after job deletion");
                }
                return false;
            })
        );

        long actualCount = countRecordsInTestIndex(index);

        // Asserts that in the last 10s, no new job ran to insert a record into the watched index & all locks are deleted for the job.
        Assert.assertEquals(1, actualCount);
    }

    public void testJobRunThenDisable() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(5L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        waitUntilLockIsAcquiredAndReleased(jobId);

        // wait till the job runner runs for the first time after 5s & inserts a record into the watched index & then delete the job.
        disableWatcherJob(jobId, jobParameter);

        // ensure log remains released as job is now descheduled
        assertThrows(
            ConditionTimeoutException.class,
            () -> await().atMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
                LockModel lock = getLockByJobId(jobId);
                if (lock != null && !lock.isReleased()) {
                    Assert.fail("Lock should not be acquired after job deletion");
                }
                return false;
            })
        );

        long actualCount = countRecordsInTestIndex(index);

        // Asserts that in the last 10s, no new job ran to insert a record into the watched index & all locks are deleted for the job.
        Assert.assertEquals(1, actualCount);

        // Cleanup
        deleteWatcherJob(jobId);
    }

    public void testJobUpdateWithRescheduleJob() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(5L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        waitUntilLockIsAcquiredAndReleased(jobId);
        Assert.assertEquals(1, countRecordsInTestIndex(index));
        // update the job params to now watch a new index.
        String newIndex = createTestIndex();
        jobParameter.setIndexToWatch(newIndex);

        // wait till the job runner runs for the first time after 1 min & inserts a record into the watched index & then update the job with
        // new params.
        createWatcherJob(jobId, jobParameter);
        waitUntilLockIsAcquiredAndReleased(jobId);

        // Asserts that the job runner has the updated params & it inserted the record in the new watched index.
        Assert.assertEquals(1, countRecordsInTestIndex(newIndex));

        // Asserts that the job runner no longer updates the old index as the job params have been updated.
        Assert.assertEquals(1, countRecordsInTestIndex(index));

        // Cleanup
        deleteWatcherJob(jobId);
    }

    public void testRunThenListJobs() throws Exception {

        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(5L);

        String[] jobIds = new String[10];
        for (int i = 0; i < 10; i++) {
            // Creates a new watcher job.
            String indexN = createTestIndex();
            SampleJobParameter jobParameterN = new SampleJobParameter();
            jobParameterN.setJobName("sample-job-it" + i);
            jobParameterN.setIndexToWatch(indexN);
            jobParameterN.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
            jobParameterN.setLockDurationSeconds(120L);

            String jobIdN = OpenSearchRestTestCase.randomAlphaOfLength(10);
            jobIds[i] = jobIdN;
            createWatcherJob(jobIdN, jobParameterN);
        }

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        waitUntilLockIsAcquiredAndReleased(jobId);
        Assert.assertEquals(1, countRecordsInTestIndex(index));

        Response response = makeRequest(client(), "GET", SCHEDULER_INFO_URI, Map.of(), null);
        Map<String, Object> responseJson = parseResponse(response);

        List<Map<String, Object>> nodes = (List<Map<String, Object>>) responseJson.get("nodes");
        assertNotNull("Nodes list should not be null", nodes);
        assertEquals(11, responseJson.get("total_jobs"));
        assertEquals(0, ((List<?>) responseJson.get("failures")).size());
        assertFalse("Should have at least one node", nodes.isEmpty());

        // Cleanup all jobs
        for (String id : jobIds) {
            deleteWatcherJob(id);
        }
        // Cleanup
        deleteWatcherJob(jobId);
    }

    public void testJobHistoryService() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        // ensures that the next job tries to run even before the previous job finished & released its lock. Also look at
        // SampleJobRunner.runTaskForLockIntegrationTests
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(10L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        waitUntilLockIsAcquiredAndReleased(jobId);

        Response response = makeRequest(client(), "GET", HISTORY_INFO_URI, Map.of(), null);
        Map<String, Object> responseJson = parseResponse(response);

        Assert.assertTrue("Response should contain total_history", responseJson.containsKey("total_history"));
        Assert.assertTrue("Response should contain history", responseJson.containsKey("history"));

        Integer totalHistory = (Integer) responseJson.get("total_history");
        Assert.assertTrue("Total history should be greater than 0", totalHistory > 0);

        Map<String, Object> history = (Map<String, Object>) responseJson.get("history");
        Assert.assertFalse("History should not be empty", history.isEmpty());

        for (Map.Entry<String, Object> entry : history.entrySet()) {
            Map<String, Object> historyRecord = (Map<String, Object>) entry.getValue();
            Assert.assertTrue("History record should contain job_index_name", historyRecord.containsKey("job_index_name"));
            Assert.assertTrue("History record should contain job_id", historyRecord.containsKey("job_id"));
            Assert.assertTrue("History record should contain start_time", historyRecord.containsKey("start_time"));
            Assert.assertTrue("History record should contain completion_status", historyRecord.containsKey("completion_status"));
            Assert.assertTrue("History record should contain end_time", historyRecord.containsKey("end_time"));

            Assert.assertEquals("Job ID should match", jobId, historyRecord.get("job_id"));
            Assert.assertEquals("Completion status should be 0", 0, historyRecord.get("completion_status"));
        }

        deleteWatcherJob(jobId);
    }

    public void testJobHistoryServiceById() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        // ensures that the next job tries to run even before the previous job finished & released its lock. Also look at
        // SampleJobRunner.runTaskForLockIntegrationTests
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(10L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        waitUntilLockIsAcquiredAndReleased(jobId);

        String historyIndexById = HISTORY_INFO_URI + "/.scheduler_sample_extension-" + jobId;

        Response response = makeRequest(client(), "GET", historyIndexById, Map.of(), null);
        Map<String, Object> responseJson = parseResponse(response);

        Assert.assertTrue("Response should contain total_history", responseJson.containsKey("total_history"));
        Assert.assertTrue("Response should contain history", responseJson.containsKey("history"));

        Integer totalHistory = (Integer) responseJson.get("total_history");
        Assert.assertTrue("Total history should be greater than 0", totalHistory > 0);

        Map<String, Object> history = (Map<String, Object>) responseJson.get("history");
        Assert.assertFalse("History should not be empty", history.isEmpty());

        for (Map.Entry<String, Object> entry : history.entrySet()) {
            Map<String, Object> historyRecord = (Map<String, Object>) entry.getValue();
            Assert.assertTrue("History record should contain job_index_name", historyRecord.containsKey("job_index_name"));
            Assert.assertTrue("History record should contain job_id", historyRecord.containsKey("job_id"));
            Assert.assertTrue("History record should contain start_time", historyRecord.containsKey("start_time"));
            Assert.assertTrue("History record should contain completion_status", historyRecord.containsKey("completion_status"));
            Assert.assertTrue("History record should contain end_time", historyRecord.containsKey("end_time"));

            Assert.assertEquals("Job ID should match", jobId, historyRecord.get("job_id"));
            Assert.assertEquals("Completion status should be 0", 0, historyRecord.get("completion_status"));
        }

        deleteWatcherJob(jobId);
    }

    public void testAcquiredLockPreventExecOfTasks() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        // ensures that the next job tries to run even before the previous job finished & released its lock. Also look at
        // SampleJobRunner.runTaskForLockIntegrationTests
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(10L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        long startTime = System.currentTimeMillis();

        waitUntilLockIsAcquiredAndReleased(jobId);

        long endTime = System.currentTimeMillis();

        long durationMs = endTime - startTime;
        Assert.assertTrue("Lock duration should be more than 10 seconds", durationMs > 10000);

        // Asserts that the job runner is running for the first time & it has inserted a new record into the watched index.
        Assert.assertEquals(1, countRecordsInTestIndex(index));

        waitUntilLockIsAcquiredAndReleased(jobId);
        Assert.assertEquals(2, countRecordsInTestIndex(index));

        // Cleanup
        deleteWatcherJob(jobId);
    }

    public void testSampleJobAcquiresALock() throws Exception {

        // Checks ability to return no locks
        Response response = makeRequest(client(), "GET", LOCK_INFO_URI, Map.of(), null);
        Map<String, Object> responseJson = parseResponse(response);

        assertEquals(0, responseJson.get("total_locks"));

        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(10L);

        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        // Run job and check for release = false
        waitUntilLockIsAcquiredAndReleased(jobId, 20, LOCK_INFO_URI, isLockReleased);

        // Checks lock is released
        response = makeRequest(client(), "GET", LOCK_INFO_URI, Map.of(), null);
        responseJson = parseResponse(response);

        // Asserts that "released" is true
        assertTrue(isLockReleased.apply(responseJson));

        // Check that .opendistro-job-scheduler-lock index is green
        Response catResponse = makeRequest(
            client(),
            "GET",
            "/_cat/indices/.opendistro-job-scheduler-lock?v&h=index,health",
            Map.of(),
            null
        );
        String catResponseBody = EntityUtils.toString(catResponse.getEntity());
        assertTrue("Lock index should be green", catResponseBody.contains("green"));

        // Cleanup
        deleteWatcherJob(jobId);
    }

    public void testSampleJobAcquiresALockPathParameter() throws Exception {

        Response response = makeRequest(client(), "GET", LOCK_INFO_URI, Map.of(), null);
        Map<String, Object> responseJson = parseResponse(response);

        assertEquals(0, responseJson.get("total_locks"));

        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(10L);

        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        // Run job and check for release = false
        waitUntilLockIsAcquiredAndReleased(jobId, 20, LOCK_INFO_URI, isLockReleased);

        // Call the scheduled info API
        // Checks lock is released
        String lock_info_URI_PathParameter = LOCK_INFO_URI + "/.scheduler_sample_extension" + "-" + jobId;

        response = makeRequest(client(), "GET", lock_info_URI_PathParameter, Map.of(), null);
        responseJson = parseResponse(response);

        // Asserts that "released" is true
        assertTrue(isLockReleased.apply(responseJson));

        // Cleanup
        deleteWatcherJob(jobId);
    }

    protected void waitUntilLockIsAcquiredAndReleased(
        String jobId,
        int maxTimeInSec,
        String LOCK_INFO_URI,
        Function<Map<String, Object>, Boolean> navigationFunction
    ) throws IOException, InterruptedException {
        AtomicLong prevLockAcquiredTime = new AtomicLong(0L);
        AtomicReference<LockModel> lock = new AtomicReference<>();

        try {
            lock.set(getLockByJobId(jobId));
            if (lock.get() != null && prevLockAcquiredTime.get() == 0L && lock.get().isReleased()) {
                prevLockAcquiredTime.set(lock.get().getLockTime().toEpochMilli());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Wait until lock is acquired (released=false)
        await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            LockModel currentLock = getLockByJobId(jobId);
            return currentLock != null && !currentLock.isReleased();
        });

        Response response = makeRequest(client(), "GET", LOCK_INFO_URI, Map.of(), null);
        Map<String, Object> responseJson = parseResponse(response);

        // Asserts that "released" is false
        assertFalse(navigationFunction.apply(responseJson));

        await().atMost(maxTimeInSec, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            lock.set(getLockByJobId(jobId));
            return lock.get() != null && lock.get().getLockTime().toEpochMilli() != prevLockAcquiredTime.get() && lock.get().isReleased();
        });
    }

    // Checks if lock is released, returns true if no locks exist (no active lock)
    @SuppressWarnings("unchecked")
    Function<Map<String, Object>, Boolean> isLockReleased = (responseJson) -> {
        Object locksObj = responseJson.get("locks");
        if (locksObj instanceof Map) {
            Map<String, Object> locks = (Map<String, Object>) locksObj;
            if (locks.isEmpty()) {
                return true; // No locks exist, so no active lock
            }
            Map<String, Object> firstLock = (Map<String, Object>) locks.values().iterator().next();
            return (boolean) firstLock.get("released");
        }
        return true; // Default to true if structure is unexpected
    };

    private Map<String, Object> parseResponse(Response response) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
    }
}
