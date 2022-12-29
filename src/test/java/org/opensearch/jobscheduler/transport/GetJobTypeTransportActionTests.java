/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.transport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
        action = new GetJobTypeTransportAction("",mock(TransportService.class),
                mock(ActionFilters.class));
        request= new GetJobTypeRequest("demo_job_type","extension_id");
        task = mock(Task.class);
        response = new ActionListener<GetJobDetailsResponse>() {
            @Override
            public void onResponse(GetJobDetailsResponse jobDetailsResponse) {
                // onResponse will not be called as we do not have the AD index
                Assert.assertEquals("success",jobDetailsResponse.getResponse());
            }

            @Override
            public void onFailure(Exception e) {
                // Assert.assertTrue(true);
            }
        };

    }

    @Test
    public void testGetJobTypeTransportAction() {
        action.doExecute(task, request, response);
    }
}

