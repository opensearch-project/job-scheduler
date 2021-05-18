/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

    public JobExecutionContext(Instant expectedExecutionTime, JobDocVersion jobVersion, LockService lockService,
                               String jobIndexName, String jobId) {
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
