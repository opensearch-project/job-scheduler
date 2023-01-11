/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestGetJobIndexActionTests extends OpenSearchTestCase {

    private RestGetJobIndexAction action;

    private JobDetailsService jobDetailsService;

    private String getJobIndexPath;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobDetailsService = Mockito.mock(JobDetailsService.class);
        action = new RestGetJobIndexAction(jobDetailsService);
        getJobIndexPath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_index");
    }

    public void testGetNames() {
        String name = action.getName();
        assertEquals(action.GET_JOB_INDEX_ACTION, name);
    }

    public void testGetRoutes() {
        List<RestHandler.Route> routes = action.routes();

        assertEquals(getJobIndexPath, routes.get(0).getPath());
    }

    public void testPrepareRequest() throws IOException {
        String content =
            "{\"job_index\":\"sample-index-name\",\"job_runner_action\":\"sample-job-runner-action\",\"job_parameter_action\":\"sample-job-parameter-action\",\"extension_unique_id\":\"sample-extension\"}";
        Map<String, String> params = new HashMap<>();
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(getJobIndexPath)
            .withParams(params)
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        Mockito.doNothing()
            .when(jobDetailsService)
            .processJobDetailsForExtensionUniqueId(
                "sample-index-name",
                null,
                "sample-job-parameter-action",
                "sample-runner-name",
                "sample-extension",
                JobDetailsService.JobDetailsRequestType.JOB_INDEX,
                ActionListener.wrap(response -> {}, exception -> {})
            );

        action.prepareRequest(request, Mockito.mock(NodeClient.class));

        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }
}
