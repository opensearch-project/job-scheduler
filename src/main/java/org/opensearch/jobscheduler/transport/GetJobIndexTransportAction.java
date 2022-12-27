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
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;

import static org.opensearch.jobscheduler.utils.RestHandlerUtils.wrapRestActionListener;

public class GetJobIndexTransportAction extends HandledTransportAction<GetJobIndexRequest,RestJobDetailsResponse> {
    private static final Logger LOG = LogManager.getLogger(GetJobIndexTransportAction.class);

    private HashMap<String,JobDetails> jobDetailsHashMap;

    private String extensionId;

    protected GetJobIndexTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters, HashMap<String, JobDetails> jobDetailsHashMap, String extensionId) {
        super(actionName, transportService, actionFilters, GetJobIndexRequest::new);
        this.jobDetailsHashMap= jobDetailsHashMap;
        this.extensionId=extensionId;
    }


    @Override
    protected void doExecute(Task task, GetJobIndexRequest request, ActionListener<RestJobDetailsResponse> actionListener) {


        ActionListener<RestJobDetailsResponse> listener = wrapRestActionListener(actionListener, "");

        try{
            JobDetails jobDetails = new JobDetails();
            jobDetails.setJobIndex(request.getJobIndex());
            jobDetails.setJobParamAction(request.getJobParamAction());
            request.setJobRunnerAction(request.getJobRunnerAction());

            jobDetailsHashMap.put(extensionId,jobDetails);

        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }
}
