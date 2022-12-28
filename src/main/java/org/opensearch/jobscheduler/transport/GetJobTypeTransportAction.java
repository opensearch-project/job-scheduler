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
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.jobscheduler.utils.RestHandlerUtils.wrapRestActionListener;

public class GetJobTypeTransportAction  extends HandledTransportAction<GetJobTypeRequest,GetJobDetailsResponse> {
    private static final Logger LOG = LogManager.getLogger(GetJobTypeTransportAction.class);
    private HashMap<String,JobDetails> jobDetailsHashMap;

    private String extensionId;

    @Inject
    public GetJobTypeTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters, HashMap<String, JobDetails> jobDetailsHashMap, String extensionId) {
        super(GetJobTypeAction.NAME, transportService, actionFilters, GetJobTypeRequest::new);
        this.jobDetailsHashMap=jobDetailsHashMap;
        this.extensionId=extensionId;
    }

    @Override
    protected void doExecute(Task task, GetJobTypeRequest request, ActionListener<GetJobDetailsResponse> actionListener) {
        ActionListener<GetJobDetailsResponse> listener = wrapRestActionListener(actionListener, "");

        try{
            JobDetails jobDetails = jobDetailsHashMap.get(extensionId);
            jobDetails.setJobType(request.getJobType());
            jobDetailsHashMap.put(extensionId,jobDetails);

            logger.info("Job Details Map size : "+jobDetailsHashMap.size() );
            for (Map.Entry<String, JobDetails> map:jobDetailsHashMap.entrySet()){
                logger.info("Key is: "+map.getValue()+" Value is : "+map.getValue().toString());
            }
        } catch (Exception e) {
//            LOG.error(e);
            listener.onFailure(e);
        }

    }
}
