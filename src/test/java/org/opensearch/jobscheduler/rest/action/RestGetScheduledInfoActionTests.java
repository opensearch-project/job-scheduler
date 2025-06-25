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

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.core.action.ActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.GetScheduledInfoAction;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestGetScheduledInfoActionTests extends OpenSearchTestCase {

    private RestGetScheduledInfoAction action;
    private String getScheduledInfoPath;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.action = new RestGetScheduledInfoAction();
        this.getScheduledInfoPath = JobSchedulerPlugin.JS_BASE_URI + "/api/jobs";
    }

    public void testGetName() {
        String name = action.getName();
        assertEquals("get_scheduled_info_action", name);
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        assertEquals(getScheduledInfoPath, routes.get(0).getPath());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
    }

    public void testPrepareRequest() throws IOException {
        // Create fake request
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(getScheduledInfoPath)
            .withParams(new HashMap<>())
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        NodeClient mockClient = Mockito.mock(NodeClient.class);

        // Mock the client.execute to invoke the listener with a response
        doAnswer(invocation -> {
            ActionListener<GetScheduledInfoResponse> listener = invocation.getArgument(2);
            GetScheduledInfoResponse response = new GetScheduledInfoResponse(new ClusterName("test-cluster"), emptyList(), emptyList());
            listener.onResponse((GetScheduledInfoResponse) response);
            return null;
        }).when(mockClient).execute(eq(GetScheduledInfoAction.INSTANCE), any(GetScheduledInfoRequest.class), any(ActionListener.class));

        // Execute the prepareRequest method
        action.prepareRequest(request, mockClient);

        // Ensure no errors thrown
        assertEquals(0, channel.responses().get());
        assertEquals(0, channel.errors().get());
    }
}
