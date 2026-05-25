/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class RunJobNodeResponseTests extends OpenSearchTestCase {

    private DiscoveryNode node;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        node = new DiscoveryNode("node1", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
    }

    public void testToXContent_withMessage() throws Exception {
        RunJobNodeResponse response = new RunJobNodeResponse(node, false, "some error");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"node_id\":\"node1\""));
        assertTrue(json.contains("\"executed\":false"));
        assertTrue(json.contains("\"message\":\"some error\""));
    }

    public void testToXContent_withoutMessage() throws Exception {
        RunJobNodeResponse response = new RunJobNodeResponse(node, true, null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"node_id\":\"node1\""));
        assertTrue(json.contains("\"executed\":true"));
        assertFalse(json.contains("\"message\""));
    }
}
