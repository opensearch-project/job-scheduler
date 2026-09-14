/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.action;

import org.opensearch.action.ActionType;
import org.opensearch.jobscheduler.transport.response.RunJobResponse;

public class RunJobAction extends ActionType<RunJobResponse> {
    public static final String NAME = "cluster:admin/opensearch/jobscheduler/jobs/run";
    public static final RunJobAction INSTANCE = new RunJobAction();

    private RunJobAction() {
        super(NAME, RunJobResponse::new);
    }
}
