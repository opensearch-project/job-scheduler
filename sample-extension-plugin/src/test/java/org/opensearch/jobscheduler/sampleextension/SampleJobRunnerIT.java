/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.sampleextension;

import org.junit.Assert;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class SampleJobRunnerIT extends SampleExtensionIntegTestCase {

    public void testJobCreateWithCorrectParams() throws IOException {
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch("http-logs");
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

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

        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        waitAndDeleteWatcherJob(schedJobParameter.getIndexToWatch(), jobId);
        long actualCount = waitAndCountRecords(index, 180000);

        Assert.assertEquals(1, actualCount);
        deleteTestIndex(index);
    }

    public void testJobUpdateWithRescheduleJob() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        SampleJobParameter schedJobParameter = createWatcherJob(jobId, jobParameter);

        String newIndex = createTestIndex();
        jobParameter.setIndexToWatch(newIndex);

        waitAndCreateWatcherJob(schedJobParameter.getIndexToWatch(), jobId, jobParameter);
        long actualCount = waitAndCountRecords(newIndex, 180000);

        Assert.assertEquals(1, actualCount);
        long prevIndexActualCount = waitAndCountRecords(index, 2000);

        Assert.assertEquals(1, prevIndexActualCount);
        deleteTestIndex(index);
        deleteTestIndex(newIndex);
    }

    public void testAcquiredLockPreventExecOfTasks() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-lock-test-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        jobParameter.setLockDurationSeconds(120L);

        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);

        long actualCount = waitAndCountRecords(index, 80000);
        Assert.assertEquals(1, actualCount);

        actualCount = waitAndCountRecords(index, 80000);
        Assert.assertEquals(1, actualCount);

        actualCount = waitAndCountRecords(index, 160000);
        Assert.assertEquals(2, actualCount);
        deleteTestIndex(index);
    }
}