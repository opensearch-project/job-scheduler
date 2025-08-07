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
import org.opensearch.jobscheduler.transport.response.GetLocksResponse;

public class GetAllLocksAction extends ActionType<GetLocksResponse> {
    public static final String NAME = "cluster:admin/opensearch/jobscheduler/locks";
    public static final GetAllLocksAction INSTANCE = new GetAllLocksAction();

    private GetAllLocksAction() {
        super(NAME, GetLocksResponse::new);
    }
}
