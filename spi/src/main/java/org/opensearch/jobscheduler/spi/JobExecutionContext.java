/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.jobscheduler.spi.utils.LockService;

import java.io.IOException;
import java.time.Instant;

public class JobExecutionContext implements Writeable {
    private final Instant expectedExecutionTime;
    private final JobDocVersion jobVersion;
    private final LockService lockService;
    private final String jobIndexName;
    private final String jobId;
    private final AuthToken accessToken;

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
        this.accessToken = null;
    }

    public JobExecutionContext(
        Instant expectedExecutionTime,
        JobDocVersion jobVersion,
        LockService lockService,
        String jobIndexName,
        String jobId,
        AuthToken accessToken
    ) {
        this.expectedExecutionTime = expectedExecutionTime;
        this.jobVersion = jobVersion;
        this.lockService = lockService;
        this.jobIndexName = jobIndexName;
        this.jobId = jobId;
        this.accessToken = accessToken;
    }

    public JobExecutionContext(StreamInput in) throws IOException {
        StreamInput nwin = new NamedWriteableAwareStreamInput(in, JobSchedulerNamedWriteableRegistry.getNamedWriteableRegistry());
        this.expectedExecutionTime = nwin.readInstant();
        this.jobVersion = new JobDocVersion(nwin);
        this.lockService = null;
        this.jobIndexName = nwin.readString();
        this.jobId = nwin.readString();
        this.accessToken = nwin.readOptionalNamedWriteable(AuthToken.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInstant(this.expectedExecutionTime);
        this.jobVersion.writeTo(out);
        out.writeString(this.jobIndexName);
        out.writeString(this.jobId);
        out.writeOptionalNamedWriteable(this.accessToken);
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

    public AuthToken getAccessToken() {
        return this.accessToken;
    }

}
