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

import java.io.IOException;

import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.ODFERestTestCase;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.jobscheduler.transport.AcquireLockResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class GetLockMultiNodeRestIT extends ODFERestTestCase {

    private String initialJobId;
    private String initialJobIndexName;
    private Response initialGetLockResponse;

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
            TestHelpers.toHttpEntity(TestHelpers.generateAcquireLockRequestBody(this.initialJobIndexName, this.initialJobId)),
            null
        );
    }

    public void testGetLockRestAPI() throws Exception {

        String initialLockId = validateResponseAndGetLockId(initialGetLockResponse);
        assertEquals(TestHelpers.generateExpectedLockId(initialJobIndexName, initialJobId), initialLockId);

        // Submit 10 requests to generate new lock models for different job indexes
        for (int i = 0; i < 10; i++) {
            Response getLockResponse = TestHelpers.makeRequest(
                client(),
                "GET",
                TestHelpers.GET_LOCK_BASE_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(TestHelpers.generateAcquireLockRequestBody(String.valueOf(i), String.valueOf(i))),
                null
            );

            String lockId = validateResponseAndGetLockId(getLockResponse);

            assertEquals(TestHelpers.generateExpectedLockId(String.valueOf(i), String.valueOf(i)), lockId);
        }
    }

    private String validateResponseAndGetLockId(Response response) throws IOException {

        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent());

        AcquireLockResponse acquireLockResponse = AcquireLockResponse.parse(parser);

        // Validate response map fields
        assertNotNull(acquireLockResponse.getLockId());
        assertNotNull(acquireLockResponse.getSeqNo());
        assertNotNull(acquireLockResponse.getPrimaryTerm());
        assertNotNull(acquireLockResponse.getLock());

        return acquireLockResponse.getLockId();
    }
}
