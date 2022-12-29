/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.rest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestGetJobTypeActionTests extends OpenSearchTestCase {

    private RestGetJobTypeAction action;

    public HashMap<String, JobDetails> jobDetailsHashMap;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobDetailsHashMap=new HashMap<>();
        action= new RestGetJobTypeAction(jobDetailsHashMap);
    }


    @Test
    public void getNameTests(){
        String name=action.getName();
        Assert.assertEquals("get_job_type_action",name);
    }


    @Test
    public void getRoutes(){
        List<RestHandler.Route> routes=action.routes();

        Assert.assertEquals("/_plugins/_job_scheduler/_get/_job_type",routes.get(0).getPath());
    }

    @Test
    public void prepareRequestTest() throws IOException {

        String content= "{\"job_type\":\"demo_job_type\",\"extension_id\":\"extension_id\"}";
        Map<String, String> params= new HashMap<>();
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
                .withPath("/_plugins/_job_scheduler/_get/_job_type")
                .withParams(params)
                .withContent(new BytesArray(content), XContentType.JSON)
                .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        action.prepareRequest(request, mock(NodeClient.class));

        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(0));
    }
}

