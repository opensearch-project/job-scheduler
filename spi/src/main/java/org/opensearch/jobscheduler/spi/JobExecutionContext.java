/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.jobscheduler.spi.utils.LockService;

import java.time.Instant;

public class JobExecutionContext {
    private final Instant expectedExecutionTime;
    private final JobDocVersion jobVersion;
    private final LockService lockService;
    private final String jobIndexName;
    private final String jobId;

    public JobExecutionContext(
        Instant expectedExecutionTime,
        JobDocVersion jobVersion,
        LockService lockService,
        String jobIndexName,
        String jobId
    ) {
        this.expectedExecutionTime = expectedExecutionTime;
        this.jobVersion = jobVersion;
        this.lockService = lockService;
        this.jobIndexName = jobIndexName;
        this.jobId = jobId;
    }

    public Instant getExpectedExecutionTime() {
        return this.expectedExecutionTime;
    }

    public JobDocVersion getJobVersion() {
        return this.jobVersion;
    }

    public LockService getLockService() {
        return this.lockService;
    }

    public String getJobIndexName() {
        return this.jobIndexName;
    }

    public String getJobId() {
        return this.jobId;
    }
}
