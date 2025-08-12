/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.model;

import java.time.Instant;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

/**
 * A {@link Writeable} ScheduledJobParameter used to transport job parameters between OpenSearch and Extensions
 *
 */
public class ExtensionJobParameter extends ScheduledJobParameter implements Writeable {

    public ExtensionJobParameter(
        String jobName,
        Schedule schedule,
        Instant lastUpdateTime,
        Instant enabledTime,
        boolean isEnabled,
        Long lockDurationSeconds,
        Double jitter
    ) {
        super(jobName, schedule, lockDurationSeconds, jitter);
        this.lastUpdateTime = lastUpdateTime;
        this.enabledTime = enabledTime;
        this.isEnabled = isEnabled;
    }

    public ExtensionJobParameter(ScheduledJobParameter jobParameter) {

        // Convert job Parameter into writeable ExtensionJobParameter
        this.name = jobParameter.getName();
        this.schedule = jobParameter.getSchedule();
        this.lastUpdateTime = jobParameter.getLastUpdateTime();
        this.enabledTime = jobParameter.getEnabledTime();
        this.isEnabled = jobParameter.isEnabled();
        this.lockDurationSeconds = jobParameter.getLockDurationSeconds();
        if (jobParameter.getJitter() != null) {
            this.jitter = jobParameter.getJitter();
        } else {
            this.jitter = 0.0;
        }
    }

}
