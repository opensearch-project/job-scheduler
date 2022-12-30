/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import static org.mockito.Mockito.mock;

public class GetJobTypeTransportActionTests extends OpenSearchTestCase {

    private GetJobTypeTransportAction action;

    private Task task;

    private GetJobTypeRequest request;

    private ActionListener<GetJobDetailsResponse> response;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new GetJobTypeTransportAction("", mock(TransportService.class), mock(ActionFilters.class));
        request = new GetJobTypeRequest("demo_job_type", "extension_id");
        task = mock(Task.class);
        response = new ActionListener<>() {
            @Override
            public void onResponse(GetJobDetailsResponse jobDetailsResponse) {
                assertEquals("success", jobDetailsResponse.getResponse());
            }

            @Override
            public void onFailure(Exception e) {
                // As the onFailure method is not triggered from the execution of doExecute method, therefore assertFalse condition is
                // defined.
                assertFalse(true);
            }
        };

    }

    public void testGetJobTypeTransportAction() {
        // do execute method with internally trigger onresponse method of the actionlistener.
        // The definition of onResponse is defined in the setup() method above.
        action.doExecute(task, request, response);
    }
}
