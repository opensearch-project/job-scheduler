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
        String jobType = restRequest.param("jobType");
        String extensionId= restRequest.param("extensionId");
        GetJobTypeRequest getJobTypeRequest = new GetJobTypeRequest(jobType, extensionId);
        RestRequest.Method method = restRequest.getHttpRequest().method();
        return channel -> client
                .execute(GetJobTypeAction.INSTANCE, getJobTypeRequest, getJobTypeResponse(channel, method,getJobTypeRequest,extensionId));

    }

    private RestResponseListener<RestJobDetailsResponse> getJobTypeResponse(
            RestChannel channel,
            RestRequest.Method method,
            GetJobTypeRequest request,
            String extensionId
    ) {
        try{
            JobDetails jobDetails = jobDetailsHashMap.get(extensionId);
            jobDetails.setJobType(request.getJobType());
            jobDetailsHashMap.put(extensionId,jobDetails);

        } catch (Exception e) {
            logger.error(e);
        }

        System.out.println("Job Details Map size : "+jobDetailsHashMap.size() );
        for (Map.Entry<String, JobDetails> map:jobDetailsHashMap.entrySet()){
            System.out.println("Key is: "+map.getValue()+" Value is : "+map.getValue().toString());
        }
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(RestJobDetailsResponse response) throws Exception {
                RestStatus restStatus = RestStatus.CREATED;
                if (method == RestRequest.Method.PUT) {
                    restStatus = RestStatus.OK;
                }
                BytesRestResponse bytesRestResponse = new BytesRestResponse(
                        restStatus,
                        response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                );
                return bytesRestResponse;
            }
        };
    }
}
