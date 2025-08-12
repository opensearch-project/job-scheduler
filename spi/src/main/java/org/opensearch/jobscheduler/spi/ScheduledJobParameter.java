/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Function;

/**
 * Job parameters that being used by the JobScheduler.
 */
public abstract class ScheduledJobParameter implements ToXContentFragment, Writeable {

    /**
     * Enum for Schedule types used to indicate which Schedule constructor to use to read from/write to the stream. Job schedules can be set via cron expression or interval.
     */
    public enum ScheduleType {
        CRON,
        INTERVAL
    }

    public static final String NAME_FIELD = "name";
    public static final String ENABLED_FILED = "enabled";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String LAST_UPDATE_TIME_FIELD_READABLE = "last_update_time_field";
    public static final String SCHEDULE_FIELD = "schedule";
    public static final String ENABLED_TIME_FILED = "enabled_time";
    public static final String ENABLED_TIME_FILED_READABLE = "enabled_time_field";
    public static final String LOCK_DURATION_SECONDS = "lock_duration_seconds";
    public static final String JITTER = "jitter";

    private static final DateFormatter STRICT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time");

    protected String name;
    protected Instant lastUpdateTime;
    protected Instant enabledTime;
    protected boolean isEnabled;
    protected Schedule schedule;
    protected Long lockDurationSeconds;
    protected Double jitter;

    /**
     * Default constructor for subclasses
     */
    public ScheduledJobParameter(String name, Schedule schedule, Long lockDurationSeconds, Double jitter) {
        this.name = name;
        this.schedule = schedule;
        this.lockDurationSeconds = lockDurationSeconds;
        this.jitter = jitter;
        Instant now = Instant.now();
        this.isEnabled = true;
        this.enabledTime = now;
        this.lastUpdateTime = now;
    }

    /**
     * Default constructor for subclasses
     */
    public ScheduledJobParameter(String name, Schedule schedule, Long lockDurationSeconds, Double jitter, boolean isEnabled) {
        this(name, schedule, lockDurationSeconds, jitter);
        this.isEnabled = isEnabled;
    }

    /**
     * Constructor for deserialization from StreamInput.
     * Subclasses should override this if they have additional fields to deserialize.
     */
    public ScheduledJobParameter(StreamInput in) throws IOException {
        this.name = in.readString();
        this.isEnabled = in.readBoolean();
        this.lockDurationSeconds = in.readOptionalLong();
        this.jitter = in.readOptionalDouble();
        this.enabledTime = in.readInstant();
        this.lastUpdateTime = in.readInstant();
        if (in.readEnum(ScheduleType.class) == ScheduleType.CRON) {
            this.schedule = new CronSchedule(in);
        } else {
            this.schedule = new IntervalSchedule(in);
        }
    }

    public ScheduledJobParameter() {}

    /**
     * @return job name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return job last update time.
     */
    public Instant getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    /**
     * @return get job enabled time.
     */
    public Instant getEnabledTime() {
        return this.enabledTime;
    }

    /**
     * @return job schedule.
     */
    public Schedule getSchedule() {
        return this.schedule;
    }

    /**
     * @return true if job is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return this.isEnabled;
    }

    /**
     * @return Null if scheduled job doesn't need lock. Seconds of lock duration if the scheduled job needs to be a singleton runner.
     */
    public Long getLockDurationSeconds() {
        return this.lockDurationSeconds;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setEnabledTime(Instant enabledTime) {
        this.enabledTime = enabledTime;
    }

    public abstract Function<StreamInput, ScheduledJobParameter> getParameterReader();

    /**
     * Job will be delayed randomly with range of (0, jitter)*interval for the
     * next execution time. For example, if next run is 10 minutes later, jitter
     * is 0.6, then next job run will be randomly delayed by 0 to 6 minutes.
     *
     * Jitter is percentage, so it should be positive and less than 1.
     * <p>
     * <b>Note:</b> default logic for these cases:
     * 1).If jitter is not set, will regard it as 0.0.
     * 2).If jitter is negative, will reset it as 0.0.
     * 3).If jitter exceeds jitter limit, will cap it as jitter limit. Default
     * jitter limit is 0.95. So if you set jitter as 0.96, will cap it as 0.95.
     *
     * @return job execution jitter
     */
    public Double getJitter() {
        return this.jitter;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(isEnabled);
        out.writeOptionalLong(lockDurationSeconds);
        out.writeOptionalDouble(jitter);
        out.writeInstant(enabledTime);
        out.writeInstant(lastUpdateTime);
        if (this.schedule instanceof CronSchedule) {
            out.writeEnum(ScheduleType.CRON);
        } else {
            out.writeEnum(ScheduleType.INTERVAL);
        }
        schedule.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME_FIELD, this.name).field(ENABLED_FILED, this.isEnabled).field(SCHEDULE_FIELD, this.schedule);
        if (this.enabledTime != null) {
            builder.timeField(ENABLED_TIME_FILED, ENABLED_TIME_FILED_READABLE, this.enabledTime.toEpochMilli());
        }
        if (this.lastUpdateTime != null) {
            builder.timeField(LAST_UPDATE_TIME_FIELD, LAST_UPDATE_TIME_FIELD_READABLE, this.lastUpdateTime.toEpochMilli());
        }
        if (this.lockDurationSeconds != null) {
            builder.field(LOCK_DURATION_SECONDS, this.lockDurationSeconds);
        }
        if (this.jitter != null) {
            builder.field(JITTER, this.jitter);
        }
        return builder;
    }
}
