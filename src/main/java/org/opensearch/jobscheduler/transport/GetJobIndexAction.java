/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionType;
import org.opensearch.jobscheduler.constant.CommonValue;

public class GetJobIndexAction extends ActionType<GetJobDetailsResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/jobIndex";
    public static final GetJobIndexAction INSTANCE = new GetJobIndexAction();

    public GetJobIndexAction() {
        super(NAME, GetJobDetailsResponse::new);
    }
}
