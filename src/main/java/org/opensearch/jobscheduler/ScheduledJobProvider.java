/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;

public class ScheduledJobProvider {
    private String jobType;
    private String jobIndexName;
    private ScheduledJobParser jobParser;
    private ScheduledJobRunner jobRunner;

    public String getJobType() {
        return jobType;
    }

    public String getJobIndexName() {
        return jobIndexName;
    }

    public ScheduledJobParser getJobParser() {
        return jobParser;
    }

    public ScheduledJobRunner getJobRunner() {
        return jobRunner;
    }

    public ScheduledJobProvider(String jobType, String jobIndexName, ScheduledJobParser jobParser, ScheduledJobRunner jobRunner) {
        this.jobType = jobType;
        this.jobIndexName = jobIndexName;
        this.jobParser = jobParser;
        this.jobRunner = jobRunner;
    }

}
