/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension.remotestore;

import org.opensearch.client.Response;
import org.opensearch.jobscheduler.sampleextension.SampleExtensionIntegTestCase;
import org.opensearch.jobscheduler.sampleextension.SampleJobParameter;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Runs against the 2-node remote-store cluster started by the {@code remoteStoreIntegTest} Gradle task (see
 * sample-extension-plugin/build.gradle). On that cluster the job index is remote-store enabled and segment
 * replicated: the replica copy never executes the delete of a job document, so the sweeper's postDelete listener
 * never fires on the replica-holding node. Jobs owned (by consistent hash) by that node used to keep running from
 * memory forever after their document was deleted. The full sweep now reconciles them.
 */
public class RemoteStoreOrphanedJobIT extends SampleExtensionIntegTestCase {

    private static final String JOB_INDEX = ".scheduler_sample_extension";
    private static final String JOBS_URI = "/_plugins/_job_scheduler/api/jobs";
    /** With one job per coin flip between the two copy holders, 16 jobs leave both nodes empty with p ~ 3e-5. */
    private static final int JOB_COUNT = 16;

    /** The remote-store repository is a system repository and cannot be deleted by the framework's cleanup. */
    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    public void testJobsDeletedWhileOwnedByReplicaHolderAreDescheduled() throws Exception {
        Set<String> jobIds = new HashSet<>();
        for (int i = 0; i < JOB_COUNT; i++) {
            String jobId = "orphan-candidate-" + i;
            createWatcherJob(jobId, newJobParameter(jobId));
            jobIds.add(jobId);
        }
        assertRemoteStoreSegmentReplicatedJobIndexOnTwoNodes();

        // every job ends up scheduled on exactly one of the two copy holders
        await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(jobIds, scheduledJobIds()));

        String primaryHolder = primaryHolderNodeId();
        Map<String, String> ownerByJob = scheduledJobOwners();
        Set<String> replicaOwned = new HashSet<>();
        Set<String> primaryOwned = new HashSet<>();
        for (Map.Entry<String, String> entry : ownerByJob.entrySet()) {
            (entry.getValue().equals(primaryHolder) ? primaryOwned : replicaOwned).add(entry.getKey());
        }
        logger.info(
            "primary holder {} owns {} jobs {}, replica holder owns {} jobs {}",
            primaryHolder,
            primaryOwned.size(),
            primaryOwned,
            replicaOwned.size(),
            replicaOwned
        );
        assertFalse("expected at least one job to be owned by the replica-holding node", replicaOwned.isEmpty());

        // the operation under test: delete every job document
        for (String jobId : jobIds) {
            deleteWatcherJob(jobId);
        }

        // postDelete handles the primary-owned jobs immediately; the replica-owned ones can only be caught by the
        // sweep's orphan reconciliation (sweeper period is 1s on this cluster)
        await().atMost(60, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals("orphaned jobs are still scheduled", Collections.emptySet(), scheduledJobIds()));

        // and the cluster is still able to schedule new jobs afterwards
        createWatcherJob("after-orphans", newJobParameter("after-orphans"));
        await().atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(Collections.singleton("after-orphans"), scheduledJobIds()));
        deleteWatcherJob("after-orphans");
    }

    public void testReAddedJobIsNotDescheduledByReconciliation() throws Exception {
        String jobId = "re-added-job";
        createWatcherJob(jobId, newJobParameter(jobId));
        await().atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(Collections.singleton(jobId), scheduledJobIds()));

        // delete and immediately re-create the same job: the new document may not be refreshed when the next sweep
        // searches the shard, so the job is an orphan candidate that the realtime GET must recognise as alive
        for (int i = 0; i < 3; i++) {
            deleteWatcherJob(jobId);
            createWatcherJob(jobId, newJobParameter(jobId));
        }

        await().atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(Collections.singleton(jobId), scheduledJobIds()));
        // stays scheduled across several further sweeps
        assertScheduledForSweeps(Collections.singleton(jobId), 5);
        deleteWatcherJob(jobId);
    }

    public void testLiveJobsAreNeverDescheduledByReconciliation() throws Exception {
        Set<String> jobIds = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            String jobId = "live-job-" + i;
            createWatcherJob(jobId, newJobParameter(jobId));
            jobIds.add(jobId);
        }
        await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(jobIds, scheduledJobIds()));
        assertScheduledForSweeps(jobIds, 5);
        for (String jobId : jobIds) {
            deleteWatcherJob(jobId);
        }
    }

    /**
     * A schedule that never fires during the test: these tests exercise scheduling / descheduling only, and the sample
     * runner is not built to cope with many concurrent executions (a failed lock acquisition takes the node down under
     * -ea). The sample REST API takes the schedule as a cron expression or an interval in seconds.
     */
    private SampleJobParameter newJobParameter(String jobId) {
        return new SampleJobParameter(
            jobId,
            "Job " + jobId,
            "watched-index",
            new CronSchedule("0 0 1 1 *", ZoneId.systemDefault()),
            30L,
            0.0
        );
    }

    /** Re-checks the scheduled set once per second for the given number of sweeps (sweeper period is 1s). */
    private void assertScheduledForSweeps(Set<String> expected, int sweeps) throws Exception {
        for (int i = 0; i < sweeps; i++) {
            Thread.sleep(1000);
            assertEquals("scheduled jobs changed unexpectedly", expected, scheduledJobIds());
        }
    }

    private void assertRemoteStoreSegmentReplicatedJobIndexOnTwoNodes() throws IOException {
        List<Map<String, Object>> nodes = catJson("/_cat/nodes?format=json&h=id,name");
        assertEquals("this test needs the 2-node remoteStoreIntegTest cluster", 2, nodes.size());

        Response response = makeRequest(
            client(),
            "GET",
            "/" + JOB_INDEX + "/_settings",
            Collections.singletonMap("flat_settings", "true"),
            null
        );
        Map<String, Object> body = entityAsMap(response);
        @SuppressWarnings("unchecked")
        Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) body.get(JOB_INDEX)).get("settings");
        assertEquals("job index must be remote-store enabled", "true", String.valueOf(settings.get("index.remote_store.enabled")));
        assertEquals("job index must be segment replicated", "SEGMENT", String.valueOf(settings.get("index.replication.type")));
        assertEquals("job index must have a replica", "1", String.valueOf(settings.get("index.number_of_replicas")));

        // both copies allocated (green), so both nodes are copy holders and candidate job owners
        makeRequest(client(), "GET", "/_cluster/health/" + JOB_INDEX, Collections.singletonMap("wait_for_status", "green"), null);
    }

    private String primaryHolderNodeId() throws IOException {
        for (Map<String, Object> shard : catJson("/_cat/shards/" + JOB_INDEX + "?format=json&h=prirep,state,id")) {
            if ("p".equals(shard.get("prirep")) && "STARTED".equals(shard.get("state"))) {
                return String.valueOf(shard.get("id"));
            }
        }
        throw new AssertionError("no started primary for " + JOB_INDEX);
    }

    private Set<String> scheduledJobIds() throws IOException {
        return new HashSet<>(scheduledJobOwners().keySet());
    }

    /** job id -> id of the node that has the job scheduled, from the by_node view of the scheduled jobs API. */
    @SuppressWarnings("unchecked")
    private Map<String, String> scheduledJobOwners() throws IOException {
        Response response = makeRequest(client(), "GET", JOBS_URI, Collections.singletonMap("by_node", "true"), null);
        Map<String, Object> body = entityAsMap(response);
        Map<String, String> owners = new HashMap<>();
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) body.get("nodes");
        if (nodes == null) {
            return owners;
        }
        for (Map<String, Object> node : nodes) {
            String nodeId = String.valueOf(node.get("node_id"));
            Map<String, Object> info = (Map<String, Object>) node.get("scheduled_job_info");
            List<Map<String, Object>> jobs = info == null ? null : (List<Map<String, Object>>) info.get("jobs");
            if (jobs == null) {
                continue;
            }
            for (Map<String, Object> job : jobs) {
                String previous = owners.put(String.valueOf(job.get("job_id")), nodeId);
                assertNull("job " + job.get("job_id") + " is scheduled on more than one node", previous);
            }
        }
        return owners;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> catJson(String endpoint) throws IOException {
        Response response = makeRequest(client(), "GET", endpoint, Collections.emptyMap(), null);
        List<Object> rows = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).list();
        List<Map<String, Object>> result = new java.util.ArrayList<>();
        for (Object row : rows) {
            result.add((Map<String, Object>) row);
        }
        return result;
    }
}
