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
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class ReleaseLockActionMultiNodeRestIT extends ODFERestTestCase {
    private Response initialGetLockResponse;
    private String initialJobId;
    private String initialJobIndexName;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.initialJobId = "testJobId";
        this.initialJobIndexName = "testJobIndexName";
        // Send initial request to ensure lock index has been created
        this.initialGetLockResponse = TestHelpers.makeRequest(
            client(),
            "GET",
            TestHelpers.GET_LOCK_BASE_URI,
            ImmutableMap.of(),
            TestHelpers.toHttpEntity(TestHelpers.generateAcquireLockRequestBody(initialJobIndexName, initialJobId)),
            null
        );
    }

    public void testReleaseLockRestAPI() throws Exception {
        String initialLockId = validateResponseAndGetLockId(entityAsMap(this.initialGetLockResponse));
        assertEquals(TestHelpers.generateExpectedLockId(initialJobIndexName, initialJobId), initialLockId);
        Response releaseLockResponse = TestHelpers.makeRequest(
            client(),
            "PUT",
            TestHelpers.RELEASE_LOCK_BASE_URI + "/" + TestHelpers.generateExpectedLockId(initialJobIndexName, initialJobId),
            ImmutableMap.of(),
            null,
            null
        );
        assertEquals("success", entityAsMap(releaseLockResponse).get("release-lock"));
    }

    private String validateResponseAndGetLockId(Map<String, Object> responseMap) {
        return (String) responseMap.get(LockModel.LOCK_ID);
    }
}
