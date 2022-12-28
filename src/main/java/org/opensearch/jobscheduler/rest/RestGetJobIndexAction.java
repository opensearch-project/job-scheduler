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
import org.opensearch.jobscheduler.transport.GetJobDetailsResponse;
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

public class RestGetJobIndexAction extends BaseRestHandler {

    private static final String GET_JOB_INDEX_ACTION = "get_job_index_action";

    private final Logger logger = LogManager.getLogger(RestGetJobIndexAction.class);

    private HashMap<String, JobDetails> jobDetailsHashMap;

    public RestGetJobIndexAction(HashMap<String, JobDetails> jobDetailsHashMap){
        this.jobDetailsHashMap=jobDetailsHashMap;
    }

    @Override
    public String getName() {
        return GET_JOB_INDEX_ACTION;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(PUT, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_index"))));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        logger.info("In Prepare request method");
        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        String jobIndex;
        String jobParamAction;
        String jobRunnerAction;
        String extensionId=null;
        try{
            jobIndex = restRequest.param("jobIndex");
            jobParamAction= restRequest.param("jobParamAction");
            jobRunnerAction= restRequest.param("jobRunnerAction");
            extensionId= restRequest.param("extensionId");
        }catch (Exception e){
            logger.info("Failed get job index for extensionId "+extensionId, e);
            return channel -> getJobIndexResponse(channel, RestStatus.BAD_REQUEST);
        }
        //GetJobIndexRequest getJobIndexRequest = new GetJobIndexRequest(jobIndex, jobParamAction,jobRunnerAction,extensionId);

        JobDetails jobDetails = jobDetailsHashMap.getOrDefault(extensionId,new JobDetails());
        jobDetails.setJobIndex(jobIndex);
        jobDetails.setJobParamAction(jobParamAction);
        jobDetails.setJobRunnerAction(jobRunnerAction);

        jobDetailsHashMap.put(extensionId,jobDetails);

        logger.info("Job Details Map size jobIndex: "+jobDetailsHashMap.size() );
        for (Map.Entry<String, JobDetails> map:jobDetailsHashMap.entrySet()){
            logger.info("Key is: "+map.getValue()+" Value is : "+map.getValue().toString());
        }

        return channel -> getJobIndexResponse(channel, RestStatus.OK);

    }

    private RestResponseListener<GetJobDetailsResponse> getJobIndexResponse(
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
