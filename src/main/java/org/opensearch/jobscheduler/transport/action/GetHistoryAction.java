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
import org.opensearch.jobscheduler.transport.response.GetHistoryResponse;

public class GetHistoryAction extends ActionType<GetHistoryResponse> {
    public static final String NAME = "cluster:admin/opensearch/jobscheduler/history";
    public static final GetHistoryAction INSTANCE = new GetHistoryAction();

    private GetHistoryAction() {
        super(NAME, GetHistoryResponse::new);
    }
}
