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
import org.opensearch.jobscheduler.rest.action.RestGetJobDetailsAction;
import org.opensearch.jobscheduler.rest.request.GetJobDetailsRequest;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestGetJobDetailsActionTests extends OpenSearchTestCase {

    private RestGetJobDetailsAction action;
    private JobDetailsService jobDetailsService;
    private String getJobDetailsPath;
    private String updateJobDetailsPath;
    private String jobIndex;
    private String jobType;
    private String jobRunnerAction;
    private String jobParameterAction;
    private String extensionUniqueId;
    private String requestBody;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.jobDetailsService = Mockito.mock(JobDetailsService.class);
        this.action = new RestGetJobDetailsAction(jobDetailsService);
        this.getJobDetailsPath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_job_details");
        this.updateJobDetailsPath = String.format(
            Locale.ROOT,
            "%s/%s/{%s}",
            JobSchedulerPlugin.JS_BASE_URI,
            "_job_details",
            GetJobDetailsRequest.DOCUMENT_ID
        );

        this.jobIndex = "sample-index-name";
        this.jobType = "sample-job-type";
        this.jobRunnerAction = "sample-job-runner-action";
        this.jobParameterAction = "sample-job-parameter-action";
        this.extensionUniqueId = "sample-extension";
        this.requestBody = "{\""
            + GetJobDetailsRequest.JOB_INDEX
            + "\":\""
            + this.jobIndex
            + "\",\""
            + GetJobDetailsRequest.JOB_TYPE
            + "\":\""
            + this.jobType
            + "\",\""
            + GetJobDetailsRequest.JOB_RUNNER_ACTION
            + "\":\""
            + this.jobRunnerAction
            + "\",\""
            + GetJobDetailsRequest.JOB_PARAMETER_ACTION
            + "\":\""
            + this.jobParameterAction
            + "\",\""
            + GetJobDetailsRequest.EXTENSION_UNIQUE_ID
            + "\":\""
            + this.extensionUniqueId
            + "\"}";
    }

    public void testGetNames() {
        String name = action.getName();
        assertEquals(action.GET_JOB_DETAILS_ACTION, name);
    }

    public void testGetRoutes() {
        List<RestHandler.Route> routes = action.routes();
        assertEquals(getJobDetailsPath, routes.get(0).getPath());
        assertEquals(updateJobDetailsPath, routes.get(1).getPath());
    }

    public void testPrepareGetJobDetailsRequest() throws IOException {

        String documentId = null;
        Map<String, String> params = new HashMap<>();

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(getJobDetailsPath)
            .withParams(params)
            .withContent(new BytesArray(requestBody), XContentType.JSON)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        Mockito.doNothing()
            .when(jobDetailsService)
            .processJobDetails(
                documentId,
                jobIndex,
                jobType,
                jobParameterAction,
                jobRunnerAction,
                extensionUniqueId,
                ActionListener.wrap(response -> {}, exception -> {})
            );

        action.prepareRequest(request, Mockito.mock(NodeClient.class));

        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }

    public void testPrepareUpdateJobDetailsRequest() throws IOException {

        String documentId = "document-id";
        Map<String, String> params = new HashMap<>();
        params.put(GetJobDetailsRequest.DOCUMENT_ID, documentId);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(updateJobDetailsPath)
            .withParams(params)
            .withContent(new BytesArray(requestBody), XContentType.JSON)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        Mockito.doNothing()
            .when(jobDetailsService)
            .processJobDetails(
                documentId,
                jobIndex,
                jobType,
                jobParameterAction,
                jobRunnerAction,
                extensionUniqueId,
                ActionListener.wrap(response -> {}, exception -> {})
            );

        action.prepareRequest(request, Mockito.mock(NodeClient.class));

        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }
}
