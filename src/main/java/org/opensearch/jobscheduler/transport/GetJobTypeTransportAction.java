/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.jobscheduler.utils.RestHandlerUtils.wrapRestActionListener;

public class GetJobTypeTransportAction  extends HandledTransportAction<GetJobTypeRequest,GetJobDetailsResponse> {
    private static final Logger LOG = LogManager.getLogger(GetJobTypeTransportAction.class);

    @Inject
    public GetJobTypeTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters) {
        super(GetJobTypeAction.NAME, transportService, actionFilters, GetJobTypeRequest::new);
    }

    @Override
    protected void doExecute(Task task, GetJobTypeRequest request, ActionListener<GetJobDetailsResponse> actionListener) {
        ActionListener<GetJobDetailsResponse> listener = wrapRestActionListener(actionListener, "Failed to fetch job type for extensionId  :"+request.getExtensionId());
        GetJobDetailsResponse response = new GetJobDetailsResponse(RestStatus.OK,"success");
        listener.onResponse(response);
    }
}
