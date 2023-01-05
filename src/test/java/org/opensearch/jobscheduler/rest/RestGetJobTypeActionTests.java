/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

public class RestGetJobTypeActionTests extends OpenSearchTestCase {

    private RestGetJobTypeAction action;

    private String getJobTypePath;

    private JobDetailsService jobDetailsService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobDetailsService = Mockito.mock(JobDetailsService.class);
        action = new RestGetJobTypeAction(jobDetailsService);
        getJobTypePath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_type");
    }

    public void testGetNames() {
        String name = action.getName();
        assertEquals(action.GET_JOB_TYPE_ACTION, name);
    }

    public void testGetRoutes() {
        List<RestHandler.Route> routes = action.routes();

        assertEquals(getJobTypePath, routes.get(0).getPath());
    }

    public void testPrepareRequest() throws IOException {

        String content = "{\"job_type\":\"sample-job-type\",\"extension_id\":\"sample-extension\"}";
        Map<String, String> params = new HashMap<>();
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(getJobTypePath)
            .withParams(params)
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        Mockito.doNothing()
            .when(jobDetailsService)
            .processJobDetailsForExtensionId(
                null,
                "sample-job-type",
                null,
                null,
                "sample-extension",
                JobDetailsService.JobDetailsRequestType.JOB_INDEX,
                ActionListener.wrap(response -> {}, exception -> {})
            );
        action.prepareRequest(request, Mockito.mock(NodeClient.class));

        assertEquals(channel.responses().get(), CoreMatchers.equalTo(0));
        assertEquals(channel.errors().get(), CoreMatchers.equalTo(0));
    }
}
