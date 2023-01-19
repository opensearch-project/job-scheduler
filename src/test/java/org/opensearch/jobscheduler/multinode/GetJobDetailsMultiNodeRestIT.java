/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.multinode;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.client.Response;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.jobscheduler.transport.GetJobDetailsRequest;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class GetJobDetailsMultiNodeRestIT extends ODFERestTestCase {

    private static final String initialRequestBody =
        "{\"job_index\":\"intial_job_index\",\"job_type\":\"intial_job_type\",\"job_parameter_action\":\"intial_job_parameter_action\",\"job_runner_action\":\"intial_job_runner_action\",\"extension_unique_id\":\"extension_unique_id\"}";
    private static final String updatedRequestBody =
        "{\"job_index\":\"updated_job_index\",\"job_type\":\"updated_job_type\",\"job_parameter_action\":\"updated_job_parameter_action\",\"job_runner_action\":\"updated_job_runner_action\",\"extension_unique_id\":\"extension_unique_id\"}";

    /**
     * The below test performs a get index api on a multinode cluster. Internally, the cluster redirects the request to either of the node.
     * After getting successful response, the get job type api is triggered for 100 times. From the response of get job type, job index is retrieved and is being compared with get index api response.
     * Both response should be equal.
     * @throws Exception
     */
    public void testGetJobDetailsRestAPI() throws Exception {

        // Send intial request
        Response response = TestHelpers.makeRequest(
            client(),
            "PUT",
            TestHelpers.GET_JOB_DETAILS_BASE_URI,
            ImmutableMap.of(),
            TestHelpers.toHttpEntity(initialRequestBody),
            null
        );

        String expectedDocumentId = validateResponseAndGetDocumentId(entityAsMap(response));

        // Submit 100 update requests
        for (int i = 0; i < 100; i++) {
            Response updateResponse = TestHelpers.makeRequest(
                client(),
                "PUT",
                TestHelpers.GET_JOB_DETAILS_BASE_URI,
                ImmutableMap.of(GetJobDetailsRequest.DOCUMENT_ID, expectedDocumentId),
                TestHelpers.toHttpEntity(updatedRequestBody),
                null
            );

            String documentId = validateResponseAndGetDocumentId(entityAsMap(updateResponse));
            assertEquals(expectedDocumentId, documentId);
        }
    }

    private String validateResponseAndGetDocumentId(Map<String, Object> responseMap) {
        assertEquals("success", responseMap.get("response"));
        return (String) responseMap.get(GetJobDetailsRequest.DOCUMENT_ID);
    }

}
