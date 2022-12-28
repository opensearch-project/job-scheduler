/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.transport.*;
import org.opensearch.rest.*;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestGetJobTypeAction extends BaseRestHandler {

    private static final String GET_JOB_TYPE_ACTION = "get_job_type_action";

    private final Logger logger = LogManager.getLogger(RestGetJobTypeAction.class);

    private HashMap<String, JobDetails> jobDetailsHashMap;

    @Override
    public String getName() {
        return GET_JOB_TYPE_ACTION;
    }

    public RestGetJobTypeAction(){}

    public RestGetJobTypeAction(HashMap<String, JobDetails> jobDetailsHashMap){
        this.jobDetailsHashMap=jobDetailsHashMap;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(PUT, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "get/_job_type"))));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        String jobType;
        String extensionId=null;
        try{
            jobType = restRequest.param("jobType");
            extensionId= restRequest.param("extensionId");
        }catch (Exception e){
            logger.info("Failed get job type for extensionId "+extensionId, e);
            return channel -> getJobTypeResponse(channel, RestStatus.BAD_REQUEST);
        }

        //GetJobTypeRequest getJobTypeRequest = new GetJobTypeRequest(jobType, extensionId);
        JobDetails jobDetails = jobDetailsHashMap.getOrDefault(extensionId,new JobDetails());
        jobDetails.setJobType(jobType);
        jobDetailsHashMap.put(extensionId,jobDetails);

        logger.info("Job Details Map size jobType: "+jobDetailsHashMap.size() );
        for (Map.Entry<String, JobDetails> map:jobDetailsHashMap.entrySet()){
            logger.info("Key is: "+map.getValue()+" Value is : "+map.getValue().toString());
        }

        return channel -> getJobTypeResponse(channel, RestStatus.OK);
    }

    private RestResponseListener<GetJobDetailsResponse> getJobTypeResponse(
            RestChannel channel,
            RestStatus status
    ) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetJobDetailsResponse response) throws Exception {
                BytesRestResponse bytesRestResponse = new BytesRestResponse(
                        status,
                        response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                );
                return bytesRestResponse;
            }
        };
    }
}
