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
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class ReleaseLockActionMultiNodeRestIT extends ODFERestTestCase {

    private Response releaseLockResponse;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.releaseLockResponse = TestHelpers.makeRequest(
            client(),
            "PUT",
            TestHelpers.RELEASE_LOCK_BASE_URI,
            ImmutableMap.of("lock_id", "lock_id"),
            null,
            null
        );
    }

    public void testGetLockRestAPI() throws Exception {
        assertEquals("success", entityAsMap(releaseLockResponse).get("response"));

        // Submit 10 requests to release locks for different job indexes
        for (int i = 0; i < 10; i++) {
            Response releaseLockResponse = TestHelpers.makeRequest(
                client(),
                "PUT",
                TestHelpers.RELEASE_LOCK_BASE_URI,
                ImmutableMap.of("lock_id", "lock_id"),
                null,
                null
            );

            assertEquals("success", entityAsMap(releaseLockResponse).get("response"));
        }
    }

}
