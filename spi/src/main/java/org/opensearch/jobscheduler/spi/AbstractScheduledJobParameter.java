/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

import java.io.IOException;
import java.time.Instant;

/**
 * Optional base class for scheduled job parameters that use the common Job Scheduler fields.
 */
public abstract class AbstractScheduledJobParameter implements ScheduledJobParameter, Writeable {
    /**
     * Enum for Schedule types used to indicate which Schedule constructor to use to read from/write to the stream.
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

    protected String name;
    protected Instant lastUpdateTime;
    protected Instant enabledTime;
    protected boolean isEnabled;
    protected Schedule schedule;
    protected Long lockDurationSeconds;
    protected Double jitter;

    protected AbstractScheduledJobParameter() {}

    protected AbstractScheduledJobParameter(String name, Schedule schedule, Long lockDurationSeconds, Double jitter) {
        this.name = name;
        this.schedule = schedule;
        this.lockDurationSeconds = lockDurationSeconds;
        this.jitter = jitter;
        Instant now = Instant.now();
        this.isEnabled = true;
        this.enabledTime = now;
        this.lastUpdateTime = now;
    }

    protected AbstractScheduledJobParameter(String name, Schedule schedule, Long lockDurationSeconds, Double jitter, boolean isEnabled) {
        this(name, schedule, lockDurationSeconds, jitter);
        this.isEnabled = isEnabled;
    }

    protected AbstractScheduledJobParameter(StreamInput in) throws IOException {
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

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Instant getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    @Override
    public Instant getEnabledTime() {
        return this.enabledTime;
    }

    @Override
    public Schedule getSchedule() {
        return this.schedule;
    }

    @Override
    public boolean isEnabled() {
        return this.isEnabled;
    }

    @Override
    public Long getLockDurationSeconds() {
        return this.lockDurationSeconds;
    }

    @Override
    public Double getJitter() {
        return this.jitter;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setEnabledTime(Instant enabledTime) {
        this.enabledTime = enabledTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this.schedule == null) {
            throw new IOException("Schedule must not be null when serializing scheduled job parameter");
        }
        out.writeString(name);
        out.writeBoolean(isEnabled);
        out.writeOptionalLong(lockDurationSeconds);
        out.writeOptionalDouble(jitter);
        out.writeInstant(enabledTime);
        out.writeInstant(lastUpdateTime);
        if (this.schedule instanceof CronSchedule) {
            out.writeEnum(ScheduleType.CRON);
        } else if (this.schedule instanceof IntervalSchedule) {
            out.writeEnum(ScheduleType.INTERVAL);
        } else {
            throw new IOException("Unsupported schedule type [" + this.schedule.getClass().getName() + "]");
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
