/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension.bwc;

import org.junit.Assert;
import org.opensearch.jobscheduler.sampleextension.SampleExtensionIntegTestCase;
import org.opensearch.jobscheduler.sampleextension.SampleJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobSchedulerBackwardsCompatibilityIT extends SampleExtensionIntegTestCase {
    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

    /*
    * In this backward compatibility test, only the old version of job-scheduler plugin is loaded in old cluster while both the latest job-scheduler
    * & sample-extension plugin is loaded in fully upgraded cluster.
     */
    @SuppressWarnings("unchecked")
    public void testBackwardsCompatibility() throws Exception {
        String uri = getPluginUri();
        assert uri != null;

        Map<String, Object> response = getAsMap(uri);
        Map<String, Map<String, Object>> responseMap = (Map<String, Map<String, Object>>) response.get("nodes");

        for (Map<String, Object> respValues : responseMap.values()) {
            List<Map<String, Object>> plugins = (List<Map<String, Object>>) respValues.get("plugins");
            List<String> pluginNames = plugins.stream().map(plugin -> plugin.get("name").toString()).collect(Collectors.toList());

            switch (CLUSTER_TYPE) {
                case OLD:
                case MIXED:
                    /*
                    * as only the old version of job-scheduler plugin is loaded, we only assert that it is loaded.
                     */
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    break;
                case UPGRADED:
                    /*
                    * As cluster is fully upgraded either by full restart or rolling upgrade, we assert, that all nodes are upgraded to use latest plugins.
                    * we trigger a call for scheduling watcher job now.
                     */
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler-sample-extension"));
                    if (CLUSTER_TYPE == ClusterType.UPGRADED || "third".equals(System.getProperty("tests.rest.bwcsuite_round"))) {
                        createBasicWatcherJob();
                    }
            }
        }
    }

    private String getPluginUri() {
        switch (CLUSTER_TYPE) {
            case OLD:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
            case MIXED: {
                return getPluginUriForMixedCluster(System.getProperty("tests.rest.bwcsuite_round"));
            }
            case UPGRADED:
                return "_nodes/plugins";
        }
        return null;
    }

    private String getPluginUriForMixedCluster(String node) {
        switch (node) {
            case "second":
                return "_nodes/" + CLUSTER_NAME + "-1/plugins";
            case "third":
                return "_nodes/" + CLUSTER_NAME + "-2/plugins";
            default:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
        }
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("Unknown cluster type: $value");
            }
        }
    }

    private void createBasicWatcherJob() throws Exception {
        String index = createTestIndex();
        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName("sample-job-it");
        jobParameter.setIndexToWatch(index);
        jobParameter.setSchedule(new IntervalSchedule(Instant.now(), 5, ChronoUnit.SECONDS));
        jobParameter.setLockDurationSeconds(5L);

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJob(jobId, jobParameter);
        waitUntilLockIsAcquiredAndReleased(jobId, 20);

        Assert.assertEquals(1, countRecordsInTestIndex(index));
    }
}
