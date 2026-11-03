/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.scheduler;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(RandomizedRunner.class)
@SuppressWarnings({ "rawtypes" })
public class JobSchedulerTests extends OpenSearchTestCase {
    private ThreadPool threadPool;

    private JobScheduler scheduler;

    private JobDocVersion dummyVersion = new JobDocVersion(1L, 1L, 1L);
    private Double jitterLimit = 0.95;

    @Before
    public void setup() {
        this.threadPool = Mockito.mock(ThreadPool.class);
        Mockito.when(this.threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        this.scheduler = new JobScheduler(this.threadPool, null);
    }

    public void testSchedule() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);

        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "job-id",
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            true
        );

        Mockito.when(schedule.getNextExecutionTime(Mockito.any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        boolean scheduled = this.scheduler.schedule("index", "job-id", jobParameter, runner, dummyVersion, jitterLimit);
        Assert.assertTrue(scheduled);
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());

        scheduled = this.scheduler.schedule("index", "job-id", jobParameter, runner, dummyVersion, jitterLimit);
        Assert.assertTrue(scheduled);
        // already scheduled, no extra threadpool call
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testSchedule_disabledJob() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "job-id",
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
        JobSchedulingInfo jobInfo = new JobSchedulingInfo("job-index", "job-id", null);
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

        JobSchedulingInfo jobInfo1 = new JobSchedulingInfo("job-index", "job-id-1", null);
        Scheduler.ScheduledCancellable scheduledCancellable1 = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo1.setScheduledCancellable(scheduledCancellable1);
        Mockito.when(scheduledCancellable1.cancel()).thenReturn(false);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id-1", jobInfo1);

        JobSchedulingInfo jobInfo2 = new JobSchedulingInfo("job-index", "job-id-2", null);
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
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name", null, null, null, false);
        Assert.assertFalse(this.scheduler.reschedule(jobParameter, null, null, dummyVersion, jitterLimit));
    }

    public void testReschedule_outOfExpectTime() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter(
            "job-id",
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            false,
            0.6
        );
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-index", "job-id", jobParameter);
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
            "job-id",
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            false,
            0.6
        );
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-index", "job-id", jobParameter);
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
            "job-id",
            "dummy job name",
            Instant.now().minus(1, ChronoUnit.HOURS),
            Instant.now(),
            schedule,
            false,
            0.6
        );
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-index", "job-id", jobParameter);
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

    public void testScheduledRunsDoNotInheritSchedulingThreadContext() throws Exception {
        // one thread, so each run finishes before the next one starts
        ThreadPool testThreadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, JobSchedulerPlugin.OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME, 1, 10, "test.scheduler")
        );
        try {
            JobScheduler jobScheduler = new JobScheduler(testThreadPool, null);
            ThreadContext threadContext = testThreadPool.getThreadContext();

            List<Object> seenTransients = new CopyOnWriteArrayList<>();
            List<String> seenHeaders = new CopyOnWriteArrayList<>();
            CountDownLatch twoRuns = new CountDownLatch(2);

            // runs every 10 ms until the first run is recorded, so the third run is an hour away and deschedule can cancel it
            Schedule schedule = Mockito.mock(Schedule.class);
            Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
                .thenAnswer(
                    invocation -> seenTransients.isEmpty() ? Instant.now().plusMillis(10) : Instant.now().plus(1, ChronoUnit.HOURS)
                );
            ScheduledJobParameter jobParameter = buildScheduledJobParameter(
                "job-id",
                "dummy job name",
                Instant.now(),
                Instant.now(),
                schedule,
                true
            );

            ScheduledJobRunner runner = (job, context) -> {
                seenTransients.add(threadContext.getTransient("test_transient"));
                seenHeaders.add(threadContext.getHeader("test_header"));
                twoRuns.countDown();
            };

            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putTransient("test_transient", "scheduling thread");
                threadContext.putHeader("test_header", "scheduling thread");

                Assert.assertTrue(jobScheduler.schedule("index", "job-id", jobParameter, runner, dummyVersion, jitterLimit));

                // the scheduling thread keeps its own context
                Assert.assertEquals("scheduling thread", threadContext.getTransient("test_transient"));
                Assert.assertEquals("scheduling thread", threadContext.getHeader("test_header"));
            }

            Assert.assertTrue("job did not run twice", twoRuns.await(10, TimeUnit.SECONDS));
            Assert.assertTrue(jobScheduler.deschedule("index", "job-id"));

            // first run, then the run scheduled from inside the first one
            Assert.assertEquals(Arrays.asList(null, null), seenTransients);
            Assert.assertEquals(Arrays.asList(null, null), seenHeaders);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    static ScheduledJobParameter buildScheduledJobParameter(
        String id,
        String name,
        Instant updateTime,
        Instant enableTime,
        Schedule schedule,
        boolean enabled
    ) {
        return buildScheduledJobParameter(id, name, updateTime, enableTime, schedule, enabled, null);
    }

    static ScheduledJobParameter buildScheduledJobParameter(
        String id,
        String name,
        Instant updateTime,
        Instant enableTime,
        Schedule schedule,
        boolean enabled,
        Double jitter
    ) {
        return new ScheduledJobParameter() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public Instant getLastUpdateTime() {
                return updateTime;
            }

            @Override
            public Instant getEnabledTime() {
                return enableTime;
            }

            @Override
            public Schedule getSchedule() {
                return schedule;
            }

            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public Double getJitter() {
                return jitter;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };
    }

}
