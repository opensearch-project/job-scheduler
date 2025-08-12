/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.scheduler;

import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(RandomizedRunner.class)
@SuppressWarnings({ "rawtypes" })
public class JobSchedulerTests extends OpenSearchTestCase {
    private ThreadPool threadPool;

    private JobScheduler scheduler;

    private JobDocVersion dummyVersion = new JobDocVersion(1L, 1L, 1L);
    private Double jitterLimit = 0.95;
    private ScheduledJobProvider provider = new ScheduledJobProvider("test", "index-name", null, null);

    @Before
    public void setup() {
        this.threadPool = Mockito.mock(ThreadPool.class);
        this.scheduler = new JobScheduler(this.threadPool, null, Map.of("index-name", provider));
    }

    public void testSchedule() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);

        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            true
        );

        Mockito.when(schedule.getNextExecutionTime(Mockito.any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        boolean scheduled = this.scheduler.schedule("index-name", "job-id", jobParameter, runner, dummyVersion, jitterLimit);
        Assert.assertTrue(scheduled);
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());

        scheduled = this.scheduler.schedule("index-name", "job-id", jobParameter, runner, dummyVersion, jitterLimit);
        Assert.assertTrue(scheduled);
        // already scheduled, no extra threadpool call
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testSchedule_disabledJob() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            new CronSchedule("* * * * *", ZoneId.systemDefault()),
            false
        );
        boolean scheduled = this.scheduler.schedule("index-name", "job-id", jobParameter, null, dummyVersion, jitterLimit);
        Assert.assertFalse(scheduled);
    }

    public void testDeschedule_singleJob() {
        JobSchedulingInfo jobInfo = new JobSchedulingInfo(provider, "job-id", null);
        Scheduler.ScheduledCancellable scheduledCancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo.setScheduledCancellable(scheduledCancellable);
        Mockito.when(scheduledCancellable.cancel()).thenReturn(false);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id", jobInfo);

        // test future.cancel return false
        boolean descheduled = this.scheduler.deschedule("index-name", "job-id");
        Assert.assertFalse(descheduled);
        Mockito.verify(scheduledCancellable).cancel();
        Assert.assertFalse(this.scheduler.getScheduledJobInfo().getJobsByIndex("index-name").isEmpty());

        // test future.cancel return true
        Mockito.when(scheduledCancellable.cancel()).thenReturn(true);
        descheduled = this.scheduler.deschedule("index-name", "job-id");
        Assert.assertTrue(descheduled);
        Mockito.verify(scheduledCancellable, Mockito.times(2)).cancel();
        Assert.assertTrue(this.scheduler.getScheduledJobInfo().getJobsByIndex("index-name").isEmpty());
    }

    public void testDeschedule_bulk() {
        Assert.assertTrue(this.scheduler.bulkDeschedule("index-name", null).isEmpty());

        JobSchedulingInfo jobInfo1 = new JobSchedulingInfo(provider, "job-id-1", null);
        Scheduler.ScheduledCancellable scheduledCancellable1 = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo1.setScheduledCancellable(scheduledCancellable1);
        Mockito.when(scheduledCancellable1.cancel()).thenReturn(false);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id-1", jobInfo1);

        JobSchedulingInfo jobInfo2 = new JobSchedulingInfo(provider, "job-id-2", null);
        Scheduler.ScheduledCancellable scheduledCancellable2 = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo2.setScheduledCancellable(scheduledCancellable2);
        Mockito.when(scheduledCancellable2.cancel()).thenReturn(true);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id-2", jobInfo2);

        List<String> ids = new ArrayList<>();
        ids.add("job-id-1");
        ids.add("job-id-2");

        List<String> result = this.scheduler.bulkDeschedule("index-name", ids);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains("job-id-1"));
        Mockito.verify(scheduledCancellable1).cancel();
        Mockito.verify(scheduledCancellable2).cancel();
    }

    public void testDeschedule_noSuchJob() {
        Assert.assertTrue(this.scheduler.deschedule("index-name", "job-id"));
    }

    public void testReschedule_noEnableTime() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("dummy job name", null, null, null, false);
        Assert.assertFalse(this.scheduler.reschedule(jobParameter, null, null, dummyVersion, jitterLimit));
    }

    public void testReschedule_outOfExpectTime() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            false,
            0.6
        );
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(provider, "job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(false);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
            .thenReturn(now.minus(10, ChronoUnit.MINUTES))
            .thenReturn(now.plus(2, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        Assert.assertTrue(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
        Assert.assertEquals(cancellable, jobSchedulingInfo.getScheduledCancellable());
        Mockito.verify(this.threadPool).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testReschedule_jobDescheduled() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            false,
            0.6
        );
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(provider, "job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(true);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
            .thenReturn(now.plus(1, ChronoUnit.MINUTES))
            .thenReturn(now.plus(2, ChronoUnit.MINUTES));

        Assert.assertFalse(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
    }

    public void testReschedule_scheduleJob() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            false,
            0.6
        );
        System.out.println("jobParameter: " + jobParameter);
        System.out.println("schedule: " + jobParameter.getSchedule());

        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(provider, "job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(false);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
            .thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES))
            .thenReturn(Instant.now().plus(2, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        Assert.assertTrue(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
        Assert.assertEquals(cancellable, jobSchedulingInfo.getScheduledCancellable());
        Mockito.verify(this.threadPool).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    static ScheduledJobParameter buildScheduledJobParameter(
        String name,
        Instant updateTime,
        Instant enableTime,
        Schedule schedule,
        boolean enabled
    ) {
        return buildScheduledJobParameter(name, updateTime, enableTime, schedule, enabled, null);
    }

    static ScheduledJobParameter buildScheduledJobParameter(
        String name,
        Instant updateTime,
        Instant enableTime,
        Schedule schedule,
        boolean enabled,
        Double jitter
    ) {
        ScheduledJobParameter parameter = new ScheduledJobParameter(name, schedule, 60L, jitter, enabled);
        parameter.setEnabledTime(enableTime);
        parameter.setLastUpdateTime(updateTime);
        return parameter;
    }

}
