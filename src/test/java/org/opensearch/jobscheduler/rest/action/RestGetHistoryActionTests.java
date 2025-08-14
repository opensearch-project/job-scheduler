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
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.GetHistoryAction;
import org.opensearch.jobscheduler.transport.request.GetHistoryRequest;
import org.opensearch.jobscheduler.transport.response.GetHistoryResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestGetHistoryActionTests extends OpenSearchTestCase {

    private RestGetHistoryAction action;
    private String getHistoryPath;
    private String getHistoryByIdPath;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.action = new RestGetHistoryAction();
        this.getHistoryPath = JobSchedulerPlugin.JS_BASE_URI + "/api/history";
        this.getHistoryByIdPath = JobSchedulerPlugin.JS_BASE_URI + "/api/history/{history_id}";
    }

    public void testGetName() {
        String name = action.getName();
        assertEquals("get_history_action", name);
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        assertEquals(2, routes.size());
        assertEquals(getHistoryPath, routes.get(0).getPath());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
        assertEquals(getHistoryByIdPath, routes.get(1).getPath());
        assertEquals(RestRequest.Method.GET, routes.get(1).getMethod());
    }

    public void testPrepareRequestWithoutHistoryId() throws IOException {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(getHistoryPath)
            .withParams(new HashMap<>())
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        NodeClient mockClient = Mockito.mock(NodeClient.class);

        doAnswer(invocation -> {
            ActionListener<GetHistoryResponse> listener = invocation.getArgument(2);
            GetHistoryResponse response = new GetHistoryResponse(new HashMap<>());
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(GetHistoryAction.INSTANCE), any(GetHistoryRequest.class), any(ActionListener.class));

        action.prepareRequest(request, mockClient);

        assertEquals(0, channel.responses().get());
        assertEquals(0, channel.errors().get());
    }

    public void testPrepareRequestWithHistoryId() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("history_id", "test-index-job123");

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(getHistoryByIdPath.replace("{history_id}", "test-index-job123"))
            .withParams(params)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        NodeClient mockClient = Mockito.mock(NodeClient.class);

        doAnswer(invocation -> {
            GetHistoryRequest historyRequest = invocation.getArgument(1);
            assertEquals("test-index-job123", historyRequest.getHistoryId());
            ActionListener<GetHistoryResponse> listener = invocation.getArgument(2);
            GetHistoryResponse response = new GetHistoryResponse(new HashMap<>());
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(GetHistoryAction.INSTANCE), any(GetHistoryRequest.class), any(ActionListener.class));

        action.prepareRequest(request, mockClient);

        assertEquals(0, channel.responses().get());
        assertEquals(0, channel.errors().get());
    }
}
