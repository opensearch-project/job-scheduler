/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.multinode;

import org.junit.Assert;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.transport.action.GetScheduledInfoAction;
import org.opensearch.jobscheduler.transport.request.GetScheduledInfoRequest;
import org.opensearch.jobscheduler.transport.response.GetScheduledInfoResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class GetScheduledInfoMultiNodeTransportIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(JobSchedulerPlugin.class);
    }

    public void testGetScheduledInfoAction() {

        // Add Job Scheduler Plugin
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());

        Assert.assertTrue(pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("opensearch-job-scheduler")));

        GetScheduledInfoRequest request = new GetScheduledInfoRequest();
        GetScheduledInfoResponse response = client().execute(GetScheduledInfoAction.INSTANCE, request).actionGet();

        assertNotNull(response);
        assertEquals(2, response.getNodes().size());

        // Count total jobs across all nodes
        int totalJobs = 0;
        for (GetScheduledInfoResponse.NodeResponse nodeResponse : response.getNodes()) {
            totalJobs += nodeResponse.getJobs().size();
        }
        assertEquals(0, totalJobs);
    }
}
