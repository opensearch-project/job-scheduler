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

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

public class ExtensionJobParameter implements ScheduledJobParameter, Writeable {

    enum ScheduleType {
        CRON,
        INTERVAL
    }

    private String extensionJobName;

    private Schedule extensionJobSchedule;

    private Instant lastUpdateTime;

    private Instant enabledTime;

    private boolean isEnabled;

    private Long lockDurationSeconds;

    private Double jitter;

    public ExtensionJobParameter(
        String extensionJobName,
        Schedule extensionJobSchedule,
        Instant lastUpdateTime,
        Instant enabledTime,
        boolean isEnabled,
        Long lockDurationSeconds,
        Double jitter
    ) {
        this.extensionJobName = extensionJobName;
        this.extensionJobSchedule = extensionJobSchedule;
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
        this.extensionJobName = in.readString();
        if (in.readEnum(ExtensionJobParameter.ScheduleType.class) == ScheduleType.CRON) {
            this.extensionJobSchedule = new CronSchedule(in);
        } else {
            this.extensionJobSchedule = new IntervalSchedule(in);
        }
        this.lastUpdateTime = in.readInstant();
        this.enabledTime = in.readInstant();
        this.isEnabled = in.readBoolean();
        this.lockDurationSeconds = in.readLong();
        this.jitter = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.extensionJobName);
        if (this.extensionJobSchedule instanceof CronSchedule) {
            out.writeEnum(ScheduleType.CRON);
        } else {
            out.writeEnum(ScheduleType.INTERVAL);
        }
        this.extensionJobSchedule.writeTo(out);
        out.writeInstant(this.lastUpdateTime);
        out.writeInstant(this.enabledTime);
        out.writeBoolean(this.isEnabled);
        out.writeLong(this.lockDurationSeconds);
        out.writeDouble(this.jitter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // no op
        return null;
    }

    @Override
    public String getName() {
        return this.extensionJobName;
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
        return this.extensionJobSchedule;
    }

    @Override
    public boolean isEnabled() {
        return this.isEnabled;
    }

}
