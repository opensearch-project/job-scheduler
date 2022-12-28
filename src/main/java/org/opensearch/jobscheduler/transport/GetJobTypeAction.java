/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionType;
import org.opensearch.jobscheduler.constant.CommonValue;

public class GetJobTypeAction extends ActionType<GetJobDetailsResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/jobType";
    public static final GetJobTypeAction INSTANCE = new GetJobTypeAction();

    public GetJobTypeAction() {
        super(NAME, GetJobDetailsResponse::new);
    }
}
