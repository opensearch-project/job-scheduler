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
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;

public class GetScheduledInfoAction extends ActionType<GetScheduledInfoResponse> {
    public static final String NAME = "cluster:admin/opensearch/_job_scheduler/info";
    public static final GetScheduledInfoAction INSTANCE = new GetScheduledInfoAction();

    private GetScheduledInfoAction() {
        super(NAME, GetScheduledInfoResponse::new);
    }
}
