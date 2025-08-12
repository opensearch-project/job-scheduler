/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;

/**
 * A sample job parameter.
 * <p>
 * It adds an additional "indexToWatch" field to {@link ScheduledJobParameter}, which stores the index
 * the job runner will watch.
 */
public class SampleJobParameter extends ScheduledJobParameter {
    public static final String INDEX_NAME_FIELD = "index_name_to_watch";

    private String indexToWatch;

    public SampleJobParameter() {}

    public SampleJobParameter(String name, String indexToWatch, Schedule schedule, Long lockDurationSeconds, Double jitter) {
        super(name, schedule, lockDurationSeconds, jitter);
        this.indexToWatch = indexToWatch;
    }

    public String getIndexToWatch() {
        return this.indexToWatch;
    }

    public void setJobName(String jobName) {
        this.name = jobName;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setEnabledTime(Instant enabledTime) {
        this.enabledTime = enabledTime;
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    public void setIndexToWatch(String indexToWatch) {
        this.indexToWatch = indexToWatch;
    }

    public void setLockDurationSeconds(Long lockDurationSeconds) {
        this.lockDurationSeconds = lockDurationSeconds;
    }

    public void setJitter(Double jitter) {
        this.jitter = jitter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.toXContent(builder, params);
        builder.field(INDEX_NAME_FIELD, this.indexToWatch);
        builder.endObject();
        return builder;
    }
}
