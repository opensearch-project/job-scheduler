/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.model;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

/**
 * A {@link Writeable} ScheduledJobParameter used to transport job parameters between OpenSearch and Extensions
 *
 */
public class ExtensionJobParameter implements ScheduledJobParameter, Writeable {

    enum ScheduleType {
        CRON,
        INTERVAL
    }

    public static final String NAME_FIELD = "name";
    public static final String SCHEDULE_FIELD = "schedule";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String ENABLED_TIME_FIELD = "enabled_time";
    public static final String IS_ENABLED_FIELD = "enabled";
    public static final String LOCK_DURATION_SECONDS_FIELD = "lock_duration_seconds";
    public static final String JITTER_FIELD = "jitter";

    private String jobName;
    private Schedule schedule;
    private Instant lastUpdateTime;
    private Instant enabledTime;
    private boolean isEnabled;

    @Nullable
    private Long lockDurationSeconds;

    @Nullable
    private Double jitter;

    public ExtensionJobParameter(
        String jobName,
        Schedule schedule,
        Instant lastUpdateTime,
        Instant enabledTime,
        boolean isEnabled,
        Long lockDurationSeconds,
        Double jitter
    ) {
        this.jobName = jobName;
        this.schedule = schedule;
        this.lastUpdateTime = lastUpdateTime;
        this.enabledTime = enabledTime;
        this.isEnabled = isEnabled;
        this.lockDurationSeconds = lockDurationSeconds;
        this.jitter = jitter;
    }

    public ExtensionJobParameter(ScheduledJobParameter jobParameter) {
        // Convert job Parameter into writeable ExtensionJobParameter
        this(
            jobParameter.getName(),
            jobParameter.getSchedule(),
            jobParameter.getLastUpdateTime(),
            jobParameter.getEnabledTime(),
            jobParameter.isEnabled(),
            jobParameter.getLockDurationSeconds(),
            jobParameter.getJitter()
        );
    }

    public ExtensionJobParameter(StreamInput in) throws IOException {
        this.jobName = in.readString();
        if (in.readEnum(ExtensionJobParameter.ScheduleType.class) == ScheduleType.CRON) {
            this.schedule = new CronSchedule(in);
        } else {
            this.schedule = new IntervalSchedule(in);
        }
        this.lastUpdateTime = in.readInstant();
        this.enabledTime = in.readInstant();
        this.isEnabled = in.readBoolean();
        this.lockDurationSeconds = in.readOptionalLong();
        this.jitter = in.readOptionalDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.jobName);
        if (this.schedule instanceof CronSchedule) {
            out.writeEnum(ScheduleType.CRON);
        } else {
            out.writeEnum(ScheduleType.INTERVAL);
        }
        this.schedule.writeTo(out);
        out.writeInstant(this.lastUpdateTime);
        out.writeInstant(this.enabledTime);
        out.writeBoolean(this.isEnabled);
        out.writeOptionalLong(this.lockDurationSeconds);
        out.writeOptionalDouble(this.jitter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD, this.jobName)
            .field(SCHEDULE_FIELD, this.schedule)
            .field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli())
            .field(ENABLED_TIME_FIELD, enabledTime.toEpochMilli())
            .field(IS_ENABLED_FIELD, isEnabled);
        if (this.lockDurationSeconds != null) {
            builder.field(LOCK_DURATION_SECONDS_FIELD, this.lockDurationSeconds);
        }
        if (this.jitter != null) {
            builder.field(JITTER_FIELD, this.jitter);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getName() {
        return this.jobName;
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

}
