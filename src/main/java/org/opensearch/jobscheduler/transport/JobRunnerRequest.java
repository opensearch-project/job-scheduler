/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import java.io.IOException;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

import java.time.Instant;

/**
 * Request to extensions to invoke their ScheduledJobRunner implementation
 */
public class JobRunnerRequest extends ExtensionActionRequest {

    public JobRunnerRequest(String extensionActionName, ScheduledJobParameter jobParameter, JobExecutionContext jobExecutionContext)
        throws IOException {
        super(extensionActionName, convertParamsToBytes(jobParameter, jobExecutionContext));
    }

    public static byte[] convertParamsToBytes(ScheduledJobParameter jobParameter, JobExecutionContext jobExecutionContext)
        throws IOException {

        // Convert job Parameter into writeable ExtensionJobParameter
        String jobName = jobParameter.getName();
        Instant lastUpdateTime = jobParameter.getLastUpdateTime();
        Instant enabledTime = jobParameter.getEnabledTime();
        Schedule schedule = jobParameter.getSchedule();
        boolean isEnabled = jobParameter.isEnabled();
        Long lockDurationSeconds = jobParameter.getLockDurationSeconds();
        Double jitter = jobParameter.getJitter();

        ExtensionJobParameter extensionJobParameter = new ExtensionJobParameter(
            jobName,
            schedule,
            lastUpdateTime,
            enabledTime,
            isEnabled,
            lockDurationSeconds,
            jitter
        );

        // Write all params to an output stream
        BytesStreamOutput out = new BytesStreamOutput();
        extensionJobParameter.writeTo(out);
        jobExecutionContext.writeTo(out);
        out.flush();

        // Convert bytes stream to byte array
        return BytesReference.toBytes(out.bytes());
    }
}
