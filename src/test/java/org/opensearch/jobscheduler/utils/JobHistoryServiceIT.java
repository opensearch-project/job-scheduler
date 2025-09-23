/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.core.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.spi.StatusHistoryModel;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JobHistoryServiceIT extends OpenSearchIntegTestCase {

    private ClusterService clusterService;
    static final String JOB_ID = "test_job_id";
    static final String JOB_INDEX_NAME = "test_job_index_name";

    @Before
    public void setup() {
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.clusterService.state().routingTable().hasIndex(JobHistoryService.JOB_HISTORY_INDEX_NAME))
            .thenReturn(false)
            .thenReturn(true);
    }

    public void testRecordJobHistorySanity() throws Exception {
        String uniqSuffix = "_record_sanity";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        String jobIndexName = "test-job-index" + uniqSuffix;
        String jobId = "test-job-id" + uniqSuffix;
        Instant startTime = Instant.now();
        Integer status = 1;
        historyService.recordJobHistory(jobIndexName, jobId, startTime, null, status, ActionListener.wrap(result -> {
            assertTrue("Failed to record job history", result);
            latch.countDown();
        }, exception -> fail("Exception occurred: " + exception.getMessage())));

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testUpdateJobHistory() throws Exception {
        String uniqSuffix = "_update_history";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        String jobIndexName = "test-job-index" + uniqSuffix;
        String jobId = "test-job-id" + uniqSuffix;
        Instant startTime = Instant.now();
        Instant endTime = startTime.plusSeconds(60);
        // First record
        historyService.recordJobHistory(jobIndexName, jobId, startTime, null, 1, ActionListener.wrap(result -> {
            assertTrue("Failed to record initial job history", result);
            // Update with end time
            historyService.recordJobHistory(jobIndexName, jobId, startTime, endTime, 2, ActionListener.wrap(updateResult -> {
                assertTrue("Failed to update job history", updateResult);
                latch.countDown();
            }, exception -> fail("Exception during update: " + exception.getMessage())));
        }, exception -> fail("Exception during initial record: " + exception.getMessage())));

        latch.await(15L, TimeUnit.SECONDS);
    }

    public void testFindHistoryRecord() throws Exception {
        String uniqSuffix = "_find_record";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        String jobIndexName = "test-job-index" + uniqSuffix;
        String jobId = "test-job-id" + uniqSuffix;
        Instant startTime = Instant.now();
        // Record first
        historyService.recordJobHistory(jobIndexName, jobId, startTime, null, 1, ActionListener.wrap(result -> {
            assertTrue("Failed to record job history", result);
            // Find the record
            historyService.findHistoryRecord(jobIndexName, jobId, startTime, ActionListener.wrap(historyModel -> {
                assertNotNull("History record should exist", historyModel);
                assertEquals("Job index name mismatch", jobIndexName, historyModel.getJobIndexName());
                assertEquals("Job ID mismatch", jobId, historyModel.getJobId());
                assertEquals("Start time mismatch", startTime.getEpochSecond(), historyModel.getStartTime().getEpochSecond());
                assertEquals("Status mismatch", 1, historyModel.getStatus());
                latch.countDown();
            }, exception -> fail("Exception during find: " + exception.getMessage())));
        }, exception -> fail("Exception during record: " + exception.getMessage())));

        latch.await(15L, TimeUnit.SECONDS);
    }

    public void testFindNonExistentRecord() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        historyService.findHistoryRecord("non-existent-index", "non-existent-job", Instant.now(), ActionListener.wrap(historyModel -> {
            assertNull("Non-existent record should return null", historyModel);
            latch.countDown();
        }, exception -> fail("Exception should not occur for non-existent record: " + exception.getMessage())));

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testRecordJobHistoryWithNullJobIndexName() throws Exception {
        String uniqSuffix = "_nullIndex";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        historyService.recordJobHistory(
            null,
            "test-job",
            Instant.now(),
            null,
            1,
            ActionListener.wrap(result -> fail("Should have failed with null job index name"), exception -> {
                assertTrue("Should be IllegalArgumentException", exception instanceof IllegalArgumentException);
                assertTrue("Should mention null parameter", exception.getMessage().contains("cannot be null"));
                latch.countDown();
            })
        );

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testRecordJobHistoryWithNullJobId() throws Exception {
        String uniqSuffix = "nullJobId";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        historyService.recordJobHistory(
            "test-index",
            null,
            Instant.now(),
            null,
            1,
            ActionListener.wrap(result -> fail("Should have failed with null job id"), exception -> {
                assertTrue("Should be IllegalArgumentException", exception instanceof IllegalArgumentException);
                assertTrue("Should mention null parameter", exception.getMessage().contains("cannot be null"));
                latch.countDown();
            })
        );

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testRecordJobHistoryWithNullStartTime() throws Exception {
        String uniqSuffix = "nullJobId";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        historyService.recordJobHistory(
            "test-index",
            "test-job",
            null,
            null,
            1,
            ActionListener.wrap(result -> fail("Should have failed with null start time"), exception -> {
                assertTrue("Should be IllegalArgumentException", exception instanceof IllegalArgumentException);
                assertTrue("Should mention null parameter", exception.getMessage().contains("cannot be null"));
                latch.countDown();
            })
        );

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testHistoryIndexCreation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        historyService.createHistoryIndex(ActionListener.wrap(created -> {
            assertTrue("History index should be created", created);
            latch.countDown();
        }, exception -> fail("Exception during index creation: " + exception.getMessage())));

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testRecordJobHistoryWithEndTime() throws Exception {
        String uniqSuffix = "_with_end_time";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        String jobIndexName = "test-job-index" + uniqSuffix;
        String jobId = "test-job-id" + uniqSuffix;
        Instant startTime = Instant.now();
        Instant endTime = startTime.plusSeconds(30);
        Integer status = 2;
        historyService.recordJobHistory(jobIndexName, jobId, startTime, endTime, status, ActionListener.wrap(result -> {
            assertTrue("Failed to record job history with end time", result);
            // Verify the record was created with end time
            historyService.findHistoryRecord(jobIndexName, jobId, startTime, ActionListener.wrap(historyModel -> {
                assertNotNull("History record should exist", historyModel);
                assertEquals("End time mismatch", endTime.getEpochSecond(), historyModel.getEndTime().getEpochSecond());
                assertEquals("Status mismatch", status.intValue(), historyModel.getStatus());
                latch.countDown();
            }, exception -> fail("Exception during find: " + exception.getMessage())));
        }, exception -> fail("Exception occurred: " + exception.getMessage())));

        latch.await(15L, TimeUnit.SECONDS);
    }

    public void testUpdateHistoryRecordDirectly() throws Exception {
        String uniqSuffix = "_direct_update";
        CountDownLatch latch = new CountDownLatch(1);
        JobHistoryService historyService = new JobHistoryService(client(), this.clusterService);
        String jobIndexName = "test-job-index" + uniqSuffix;
        String jobId = "test-job-id" + uniqSuffix;
        Instant startTime = Instant.now();
        // First create a record
        historyService.recordJobHistory(jobIndexName, jobId, startTime, null, 1, ActionListener.wrap(result -> {
            assertTrue("Failed to record initial job history", result);
            // Find the record to get seq_no and primary_term
            historyService.findHistoryRecord(jobIndexName, jobId, startTime, ActionListener.wrap(historyModel -> {
                assertNotNull("History record should exist", historyModel);
                // Create updated model
                StatusHistoryModel updatedModel = new StatusHistoryModel(jobIndexName, jobId, startTime, Instant.now(), 3);
                // Update directly
                historyService.updateHistoryRecord(updatedModel, ActionListener.wrap(updatedHistoryModel -> {
                    assertNotNull("Updated history model should not be null", updatedHistoryModel);
                    assertEquals("Status should be updated", 3, updatedHistoryModel.getStatus());
                    assertNotNull("End time should be set", updatedHistoryModel.getEndTime());
                    latch.countDown();
                }, exception -> fail("Exception during update: " + exception.getMessage())));
            }, exception -> fail("Exception during find: " + exception.getMessage())));
        }, exception -> fail("Exception during initial record: " + exception.getMessage())));

        latch.await(20L, TimeUnit.SECONDS);
    }
}
