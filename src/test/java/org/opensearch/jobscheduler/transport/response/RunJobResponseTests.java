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
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RunJobResponseTests extends OpenSearchTestCase {

    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private ClusterName clusterName;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        node1 = new DiscoveryNode("node1", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode("node2", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        clusterName = new ClusterName("test-cluster");
    }

    public void testGetExecutingNode_noNodeExecuted_returnsNull() {
        List<RunJobNodeResponse> nodes = Arrays.asList(
            new RunJobNodeResponse(node1, false, "not scheduled"),
            new RunJobNodeResponse(node2, false, "not scheduled")
        );
        RunJobResponse response = new RunJobResponse(clusterName, nodes, Collections.emptyList());
        assertNull(response.getExecutingNode());
    }

    public void testGetExecutingNode_oneNodeExecuted_returnsThatNode() {
        List<RunJobNodeResponse> nodes = Arrays.asList(
            new RunJobNodeResponse(node1, false, "not scheduled"),
            new RunJobNodeResponse(node2, true, null)
        );
        RunJobResponse response = new RunJobResponse(clusterName, nodes, Collections.emptyList());
        RunJobNodeResponse executingNode = response.getExecutingNode();
        assertNotNull(executingNode);
        assertEquals("node2", executingNode.getNode().getId());
    }

    public void testGetExecutingNode_emptyNodeList_returnsNull() {
        RunJobResponse response = new RunJobResponse(clusterName, Collections.emptyList(), Collections.emptyList());
        assertNull(response.getExecutingNode());
    }

    public void testToXContent_withExecutingNode() throws Exception {
        List<RunJobNodeResponse> nodes = Collections.singletonList(new RunJobNodeResponse(node1, true, null));
        RunJobResponse response = new RunJobResponse(clusterName, nodes, Collections.emptyList());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"executed\":true"));
        assertTrue(json.contains("\"executing_node_id\":\"node1\""));
        assertTrue(json.contains("\"executing_node_name\""));
        assertTrue(json.contains("\"nodes\""));
        assertTrue(json.contains("\"failures\""));
    }
}
