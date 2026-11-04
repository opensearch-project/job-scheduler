/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs on the deliberately slow remote-store cluster configured by {@code remoteStoreLockIntegTest}. The repository
 * pauses each remote blob write, turning the synchronous remote refresh performed by {@code IMMEDIATE} into a
 * deterministic system-write queue blocker.
 */
public class RemoteStoreLockServiceIT extends OpenSearchIntegTestCase {

    private static final long LOCK_DURATION_SECONDS = 60;
    private ClusterService clusterService;

    @Override
    protected Settings externalClusterClientSettings() {
        String repositoryRoot = System.getProperty("tests.job_scheduler.remote_store.repository");
        if (repositoryRoot == null) {
            throw new IllegalStateException("tests.job_scheduler.remote_store.repository must be set for this test");
        }
        Settings.Builder settings = Settings.builder().put("path.repo", repositoryRoot);
        for (String repositoryName : List.of("job-scheduler-segment-repo", "job-scheduler-translog-repo", "job-scheduler-state-repo")) {
            settings.put("node.attr.remote_store.repository." + repositoryName + ".type", "reloadable-fs");
            settings.put(
                "node.attr.remote_store.repository." + repositoryName + ".settings.location",
                repositoryRoot + "/" + repositoryName
            );
            settings.put("node.attr.remote_store.repository." + repositoryName + ".settings.repositories.slowdown", "1");
        }
        return settings.put("node.attr.remote_store.segment.repository", "job-scheduler-segment-repo")
            .put("node.attr.remote_store.translog.repository", "job-scheduler-translog-repo")
            .put("node.attr.remote_store.state.repository", "job-scheduler-state-repo")
            .build();
    }

    @Before
    public void setUpLockService() {
        clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.state().routingTable().hasIndex(LockServiceImpl.LOCK_INDEX_NAME)).thenReturn(false).thenReturn(true);
    }

    public void testRealtimeLockLifecycleDoesNotRequireRefresh() throws Exception {
        LockServiceImpl lockService = new LockServiceImpl(client(), clusterService);
        createLockIndex(lockService);
        assertRemoteStoreBackedLockIndex();
        client().admin()
            .indices()
            .prepareUpdateSettings(LockServiceImpl.LOCK_INDEX_NAME)
            .setSettings(Settings.builder().put("index.refresh_interval", "-1"))
            .get();

        String lockId = "realtime-lock";
        LockModel acquired = acquire(lockService, lockId);
        assertEquals(0L, client().prepareSearch(LockServiceImpl.LOCK_INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());

        LockModel observed = find(lockService, acquired.getLockId());
        assertNotNull("realtime GET must see an unrefreshed lock", observed);
        assertFalse("lock must be active", observed.isReleased());
        assertTrue(release(lockService, observed));

        LockModel released = find(lockService, acquired.getLockId());
        assertNotNull("realtime GET must see the unrefreshed release", released);
        assertTrue("lock must be released", released.isReleased());
        assertEquals(0L, client().prepareSearch(LockServiceImpl.LOCK_INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());

        client().admin().indices().prepareRefresh(LockServiceImpl.LOCK_INDEX_NAME).get();
        assertEquals(1L, client().prepareSearch(LockServiceImpl.LOCK_INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
    }

    public void testForcedRefreshLockWritesFillSystemWriteQueue() throws Exception {
        LockServiceImpl lockService = new LockServiceImpl(client(), clusterService);
        createLockIndex(lockService);
        assertRemoteStoreBackedLockIndex();

        // Model the previous remote-store LockService create path with RefreshPolicy.IMMEDIATE.
        List<PlainActionFuture<IndexResponse>> requests = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            LockModel lock = new LockModel(
                "slow-remote-store-job-index",
                "forced-refresh-" + i,
                Instant.now(),
                LOCK_DURATION_SECONDS,
                false
            );
            IndexRequest request = new IndexRequest(LockServiceImpl.LOCK_INDEX_NAME).id(lock.getLockId())
                .source(lock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .create(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
            client().index(request, future);
            requests.add(future);
        }

        assertBusy(() -> {
            ThreadPoolStats.Stats stats = systemWriteStats();
            assertTrue("a forced refresh should occupy the single system_write worker", stats.getActive() > 0);
            assertTrue("a forced refresh should queue subsequent lock writes", stats.getQueue() > 0);
        }, 30, TimeUnit.SECONDS);

        for (PlainActionFuture<IndexResponse> request : requests) {
            request.get(90, TimeUnit.SECONDS);
        }
    }

    private void createLockIndex(LockServiceImpl lockService) {
        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        lockService.createLockIndex(future);
        assertTrue("failed to create lock index", future.actionGet());
        Mockito.when(clusterService.state().metadata()).thenReturn(client().admin().cluster().prepareState().get().getState().metadata());
    }

    private LockModel acquire(LockServiceImpl lockService, String lockId) {
        PlainActionFuture<LockModel> future = PlainActionFuture.newFuture();
        lockService.acquireLockWithId("slow-remote-store-job-index", LOCK_DURATION_SECONDS, lockId, future);
        LockModel lock = future.actionGet();
        assertNotNull("expected lock acquisition", lock);
        return lock;
    }

    private LockModel find(LockServiceImpl lockService, String lockId) {
        PlainActionFuture<LockModel> future = PlainActionFuture.newFuture();
        lockService.findLock(lockId, future);
        return future.actionGet();
    }

    private boolean release(LockServiceImpl lockService, LockModel lock) {
        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        lockService.release(lock, future);
        return future.actionGet();
    }

    private void assertRemoteStoreBackedLockIndex() {
        Settings settings = client().admin()
            .indices()
            .prepareGetSettings(LockServiceImpl.LOCK_INDEX_NAME)
            .get()
            .getIndexToSettings()
            .get(LockServiceImpl.LOCK_INDEX_NAME);
        assertEquals("true", settings.get("index.remote_store.enabled"));
        assertEquals("SEGMENT", settings.get("index.replication.type"));
    }

    private ThreadPoolStats.Stats systemWriteStats() {
        NodesStatsResponse response = client().admin()
            .cluster()
            .prepareNodesStats()
            .clear()
            .addMetric(NodesStatsRequest.Metric.THREAD_POOL.metricName())
            .get();
        for (NodeStats nodeStats : response.getNodes()) {
            for (ThreadPoolStats.Stats stats : nodeStats.getThreadPool()) {
                if (ThreadPool.Names.SYSTEM_WRITE.equals(stats.getName()) && stats.getThreads() == 1) {
                    return stats;
                }
            }
        }
        throw new AssertionError("single-threaded system_write pool was not reported");
    }
}
