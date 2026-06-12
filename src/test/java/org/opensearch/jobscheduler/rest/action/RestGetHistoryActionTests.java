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

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.action = new RestGetHistoryAction();
        this.getHistoryPath = JobSchedulerPlugin.JS_BASE_URI + "/api/history";
    }

    public void testGetName() {
        String name = action.getName();
        assertEquals("get_history_action", name);
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        assertEquals(getHistoryPath, routes.get(0).getPath());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
    }

    public void testPrepareRequestWithoutJobFilter() throws IOException {
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

    public void testPrepareRequestWithJobFilter() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("job_index_name", "test-index");
        params.put("job_id", "job123");

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(getHistoryPath)
            .withParams(params)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        NodeClient mockClient = Mockito.mock(NodeClient.class);

        doAnswer(invocation -> {
            GetHistoryRequest historyRequest = invocation.getArgument(1);
            assertEquals("test-index", historyRequest.getJobIndexName());
            assertEquals("job123", historyRequest.getJobId());
            ActionListener<GetHistoryResponse> listener = invocation.getArgument(2);
            GetHistoryResponse response = new GetHistoryResponse(new HashMap<>());
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(GetHistoryAction.INSTANCE), any(GetHistoryRequest.class), any(ActionListener.class));

        action.prepareRequest(request, mockClient);

        assertEquals(0, channel.responses().get());
        assertEquals(0, channel.errors().get());
    }

    public void testGetHistoryRequestValidationRequiresCompleteJobFilter() {
        assertNull(new GetHistoryRequest(null, null).validate());
        assertNull(new GetHistoryRequest("test-index", "job123").validate());
        assertNotNull(new GetHistoryRequest("test-index", null).validate());
        assertNotNull(new GetHistoryRequest(null, "job123").validate());
    }
}
