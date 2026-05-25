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
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.RunJobAction;
import org.opensearch.jobscheduler.transport.request.RunJobRequest;
import org.opensearch.jobscheduler.transport.response.RunJobResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestRunJobActionTests extends OpenSearchTestCase {

    private RestRunJobAction action;
    private String runJobPath;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.action = new RestRunJobAction();
        this.runJobPath = String.format(
            Locale.ROOT,
            "%s/_run/{%s}/{%s}",
            JobSchedulerPlugin.JS_BASE_URI,
            RestRunJobAction.JOB_TYPE,
            RestRunJobAction.JOB_ID
        );
    }

    public void testGetName() {
        assertEquals("run_job_action", action.getName());
    }

    public void testRoutes_singlePostRoute() {
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(runJobPath, routes.get(0).getPath());
    }

    public void testPrepareRequest_invokesExecuteOnClient() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(RestRunJobAction.JOB_TYPE, "scheduler_sample_extension");
        params.put(RestRunJobAction.JOB_ID, "my-job");

        String path = String.format(
            Locale.ROOT,
            "%s/_run/scheduler_sample_extension/my-job",
            JobSchedulerPlugin.JS_BASE_URI
        );
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.POST)
            .withPath(path)
            .withParams(params)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        NodeClient mockClient = Mockito.mock(NodeClient.class);

        doAnswer(invocation -> {
            ActionListener<RunJobResponse> listener = invocation.getArgument(2);
            RunJobResponse response = new RunJobResponse(
                new ClusterName("test-cluster"),
                Collections.emptyList(),
                Collections.emptyList()
            );
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(RunJobAction.INSTANCE), any(RunJobRequest.class), any(ActionListener.class));

        action.prepareRequest(request, mockClient);

        assertEquals(0, channel.responses().get());
        assertEquals(0, channel.errors().get());
    }
}
