/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.rest.request.GetAllJobInfoRequest;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestGetAllJobInfoActionTests extends OpenSearchTestCase {

    private RestGetAllJobInfoAction action;
    private JobDetailsService jobDetailsService;
    private String getAllJobInfoPath;
    private String requestBody;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.jobDetailsService = Mockito.mock(JobDetailsService.class);
        this.action = new RestGetAllJobInfoAction(jobDetailsService);
        this.getAllJobInfoPath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_jobs");
        
        // Create request body with activeJobsOnly parameter
        this.requestBody = "{\"" + GetAllJobInfoRequest.ACTIVE_JOBS_ONLY + "\":true}";
    }

    public void testGetName() {
        String name = action.getName();
        assertEquals(RestGetAllJobInfoAction.GET_ALL_JOB_INFO_ACTION, name);
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        assertEquals(getAllJobInfoPath, routes.get(0).getPath());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
    }

    public void testPrepareRequest() throws IOException {
        // Create fake request
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath(getAllJobInfoPath)
            .withParams(new HashMap<>())
            .withContent(new BytesArray(requestBody), XContentType.JSON)
            .build();
        
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        
        // Execute the prepareRequest method
        action.prepareRequest(request, Mockito.mock(NodeClient.class));
        
        // Since the actual response is handled asynchronously, we can only verify
        // that the method doesn't throw exceptions
        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }
}