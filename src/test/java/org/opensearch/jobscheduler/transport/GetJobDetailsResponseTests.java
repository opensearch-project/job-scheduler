/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import org.junit.Test;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GetJobDetailsResponseTests extends OpenSearchTestCase {

    @Test
    public void testToXContent() throws IOException {
        GetJobDetailsResponse getJobDetailsResponse = new GetJobDetailsResponse(RestStatus.OK, "success");
        String response = TestHelpers.xContentBuilderToString(
            getJobDetailsResponse.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
        );
        assertEquals("{\"response\":\"success\"}", response);
    }

}
