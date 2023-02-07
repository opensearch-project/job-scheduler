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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.opensearch.client.Response;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.jobscheduler.rest.RestGetLockAction;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class GetLockMultiNodeRestIT extends ODFERestTestCase {

    public void testGetLockRestAPI() throws Exception {

        String intialJobId = "testjobId";
        String initialJobIndexName = "testJobIndexName";

        // Send initial request to ensure lock index has been created
        Response response = TestHelpers.makeRequest(
            client(),
            "GET",
            TestHelpers.GET_LOCK_BASE_URI,
            ImmutableMap.of(),
            TestHelpers.toHttpEntity(generateRequestBody(initialJobIndexName, intialJobId)),
            null
        );

        String initialLockId = validateResponseAndGetLockId(entityAsMap(response));
        assertEquals(generateExpectedLockId(initialJobIndexName, intialJobId), initialLockId);

        // Submit 100 requests to generate new lock models for different job indexes
        for (int i = 0; i < 100; i++) {
            Response getLockResponse = TestHelpers.makeRequest(
                client(),
                "GET",
                TestHelpers.GET_LOCK_BASE_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(generateRequestBody(String.valueOf(i), String.valueOf(i))),
                null
            );

            String lockId = validateResponseAndGetLockId(entityAsMap(getLockResponse));
            assertEquals(generateExpectedLockId(String.valueOf(i), String.valueOf(i)), lockId);
        }
    }

    private String validateResponseAndGetLockId(Map<String, Object> responseMap) {
        assertEquals("success", responseMap.get("response"));
        return (String) responseMap.get(RestGetLockAction.LOCK_ID);
    }

    private String generateRequestBody(String jobIndexName, String jobId) {
        return "{\"job_id\":\"" + jobId + "\",\"job_index_name\":\"" + jobIndexName + "\",\"lock_duration_seconds\":\"30.0\"}";
    }

    private String generateExpectedLockId(String jobIndexName, String jobId) {
        return jobIndexName + "-" + jobId;
    }

}
