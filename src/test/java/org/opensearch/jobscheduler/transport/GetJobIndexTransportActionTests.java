/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import static org.mockito.Mockito.mock;

public class GetJobIndexTransportActionTests extends OpenSearchTestCase {

    private GetJobIndexTransportAction action;

    private Task task;

    private GetJobIndexRequest request;

    private ActionListener<GetJobDetailsResponse> response;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new GetJobIndexTransportAction("", mock(TransportService.class), mock(ActionFilters.class));
        request = new GetJobIndexRequest("demo_job_index", "job_parser_action", "job_runner_action", "extension_id");
        task = mock(Task.class);
        response = new ActionListener<GetJobDetailsResponse>() {
            @Override
            public void onResponse(GetJobDetailsResponse jobDetailsResponse) {
                // onResponse will not be called as we do not have the AD index
                Assert.assertEquals("success", jobDetailsResponse.getResponse());
            }

            @Override
            public void onFailure(Exception e) {
                // Assert.assertTrue(true);
            }
        };

    }

    public void testGetJobIndexTransportAction() {
        action.doExecute(task, request, response);
    }
}
