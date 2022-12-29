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
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.jobscheduler.utils.RestHandlerUtils.wrapRestActionListener;

public class GetJobIndexTransportAction extends HandledTransportAction<GetJobIndexRequest,GetJobDetailsResponse> {
    private static final Logger logger = LogManager.getLogger(GetJobIndexTransportAction.class);



    @Inject
    public GetJobIndexTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters) {
        super(GetJobIndexAction.NAME, transportService, actionFilters, GetJobIndexRequest::new);
    }


    @Override
    protected void doExecute(Task task, GetJobIndexRequest request, ActionListener<GetJobDetailsResponse> actionListener) {

        ActionListener<GetJobDetailsResponse> listener = wrapRestActionListener(actionListener, "Failed to fetch job index for extensionId  :"+request.getExtensionId());
        GetJobDetailsResponse response = new GetJobDetailsResponse(RestStatus.OK,"success");
        listener.onResponse(response);
    }
}
