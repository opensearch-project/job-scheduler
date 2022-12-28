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
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.jobscheduler.utils.RestHandlerUtils.wrapRestActionListener;

public class GetJobIndexTransportAction extends HandledTransportAction<GetJobIndexRequest,GetJobDetailsResponse> {
    private static final Logger logger = LogManager.getLogger(GetJobIndexTransportAction.class);

    private HashMap<String,JobDetails> jobDetailsHashMap;

    private String extensionId;


    @Inject
    public GetJobIndexTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters, HashMap<String, JobDetails> jobDetailsHashMap, String extensionId) {
        super(GetJobIndexAction.NAME, transportService, actionFilters, GetJobIndexRequest::new);
        this.jobDetailsHashMap= jobDetailsHashMap;
        this.extensionId=extensionId;
    }


    @Override
    protected void doExecute(Task task, GetJobIndexRequest request, ActionListener<GetJobDetailsResponse> actionListener) {


        ActionListener<GetJobDetailsResponse> listener = wrapRestActionListener(actionListener, "");

        try{
            JobDetails jobDetails = new JobDetails();
            jobDetails.setJobIndex(request.getJobIndex());
            jobDetails.setJobParamAction(request.getJobParamAction());
            request.setJobRunnerAction(request.getJobRunnerAction());

            jobDetailsHashMap.put(extensionId,jobDetails);

            logger.info("Job Details Map size : "+jobDetailsHashMap.size() );
            for (Map.Entry<String, JobDetails> map:jobDetailsHashMap.entrySet()){
                logger.info("Key is: "+map.getValue()+" Value is : "+map.getValue().toString());
            }

        } catch (Exception e) {
            //logger.info(e);
            listener.onFailure(e);
        }

    }
}
