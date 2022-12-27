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
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;

import static org.opensearch.jobscheduler.utils.RestHandlerUtils.wrapRestActionListener;

public class GetJobTypeTransportAction  extends HandledTransportAction<GetJobTypeRequest,RestJobDetailsResponse> {
    private static final Logger LOG = LogManager.getLogger(GetJobTypeTransportAction.class);
    private HashMap<String,JobDetails> jobDetailsHashMap;

    private String extensionId;

    protected GetJobTypeTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters, Writeable.Reader<GetJobTypeRequest> getJobTypeRequestReader, HashMap<String, JobDetails> jobDetailsHashMap, String extensionId) {
        super(actionName, transportService, actionFilters, getJobTypeRequestReader);
        this.jobDetailsHashMap=jobDetailsHashMap;
        this.extensionId=extensionId;
    }

    @Override
    protected void doExecute(Task task, GetJobTypeRequest request, ActionListener<RestJobDetailsResponse> actionListener) {
        ActionListener<RestJobDetailsResponse> listener = wrapRestActionListener(actionListener, "");

        try{
            JobDetails jobDetails = jobDetailsHashMap.get(extensionId);
            jobDetails.setJobType(request.getJobType());
            jobDetailsHashMap.put(extensionId,jobDetails);
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }
}
