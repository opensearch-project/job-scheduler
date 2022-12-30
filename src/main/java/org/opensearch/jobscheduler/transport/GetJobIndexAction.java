/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionType;
import org.opensearch.jobscheduler.constant.CommonValue;

/**
 * Get Job Index action
 */
public class GetJobIndexAction extends ActionType<GetJobDetailsResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/jobIndex";
    public static final GetJobIndexAction INSTANCE = new GetJobIndexAction();

    public GetJobIndexAction() {
        super(NAME, GetJobDetailsResponse::new);
    }
}
