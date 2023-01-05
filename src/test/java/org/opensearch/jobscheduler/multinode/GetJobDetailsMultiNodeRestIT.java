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
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.opensearch.client.Response;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class GetJobDetailsMultiNodeRestIT extends ODFERestTestCase {

    /**
     * The below test performs a get index api on a multinode cluster. Internally, the cluster redirects the request to either of the node.
     * After getting successful response, the get job type api is triggered for 100 times. From the response of get job type, job index is retrieved and is being compared with get index api response.
     * Both response should be equal.
     * @throws Exception
     */
    public void testGetJobDetailsRestAPI() throws Exception {

        Response response = TestHelpers.makeRequest(
            client(),
            "PUT",
            TestHelpers.GET_JOB_INDEX_BASE_DETECTORS_URI,
            ImmutableMap.of(),
            TestHelpers.toHttpEntity(
                "{\"job_index\":\"demo_job_index\",\"job_parameter_action\":\"demo_parameter\",\"job_runner_action\":\"demo_runner\",\"extension_id\":\"sample_extension\"}"
            ),
            null
        );

        String expectedJobIndex = validateResponseAndGetJobIndex(entityAsMap(response));

        for (int i = 0; i < 100; i++) {
            Response response1 = TestHelpers.makeRequest(
                client(),
                "PUT",
                TestHelpers.GET_JOB_TYPE_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity("{\"job_type\":\"demo_job_type\",\"extension_id\":\"sample_extension\"}"),
                null
            );

            String jobIndex = validateResponseAndGetJobIndex(entityAsMap(response1));
            assertEquals(expectedJobIndex, jobIndex);
        }
    }

    private String validateResponseAndGetJobIndex(Map<String, Object> responseMap) {
        assertEquals("success", responseMap.get("response"));
        HashMap<String, String> jobDetails = (HashMap<String, String>) responseMap.get("jobDetails");
        return jobDetails.get("job_index");
    }

}
