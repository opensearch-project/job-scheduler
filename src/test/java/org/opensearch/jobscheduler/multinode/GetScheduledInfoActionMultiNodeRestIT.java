/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.multinode;

import java.io.IOException;
import java.util.Map;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class GetScheduledInfoActionMultiNodeRestIT extends ODFERestTestCase {

    private static final String SCHEDULER_INFO_URI = "/_plugins/_job_scheduler/info";

    public void testGetScheduledInfoRestAPI() throws Exception {
        // Check cluster health first

        Response healthResponse = TestHelpers.makeRequest(client(), "GET", "/_cluster/health", Map.of(), null, null);

        Map<String, Object> healthMap = parseResponseAsMap(healthResponse);
        assertEquals("green", healthMap.get("status"));

        // Test with default parameters
        Response response = TestHelpers.makeRequest(client(), "GET", SCHEDULER_INFO_URI, Map.of(), null, null);

        Map<String, Object> responseMap = parseResponseAsMap(response);

        // Verify response structure
        assertTrue("Response should contain jobs array", responseMap.containsKey("jobs"));
        assertTrue("Response should contain total_jobs count", responseMap.containsKey("total_jobs"));

        // Test with by_node parameter set to true
        Response responseByNode = TestHelpers.makeRequest(client(), "GET", SCHEDULER_INFO_URI, Map.of("by_node", "true"), null, null);

        Map<String, Object> responseByNodeMap = parseResponseAsMap(responseByNode);

        // Verify response structure when grouped by node
        assertTrue("Response should contain nodes object", responseByNodeMap.containsKey("nodes"));
        assertTrue("Response should contain total_jobs count", responseByNodeMap.containsKey("total_jobs"));
    }

    private Map<String, Object> parseResponseAsMap(Response response) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent());
        return parser.map();
    }
}
