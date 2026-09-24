/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sweeper;

import org.opensearch.jobscheduler.JobSchedulerSettings;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.utils.LockServiceImpl;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class JobSweeperTests extends OpenSearchAllocationTestCase {

    private Client client;
    private ClusterService clusterService;
    private NamedXContentRegistry xContentRegistry;
    private ThreadPool threadPool;
    private JobScheduler scheduler;
    private Settings settings;
    private ScheduledJobParser jobParser;
    private ScheduledJobRunner jobRunner;

    private JobSweeper sweeper;
    private JobDetailsService jobDetailsService;

    private DiscoveryNode discoveryNode;

    private Double jitterLimit = 0.95;

    @Before
    public void setup() throws IOException {
        this.client = Mockito.mock(Client.class);
        this.threadPool = Mockito.mock(ThreadPool.class);
        this.scheduler = Mockito.mock(JobScheduler.class);
        this.jobRunner = Mockito.mock(ScheduledJobRunner.class);
        this.jobParser = Mockito.mock(ScheduledJobParser.class);
        this.jobDetailsService = Mockito.mock(JobDetailsService.class);

        // NamedXContentRegistry.Entry xContentRegistryEntry = new NamedXContentRegistry.Entry(ScheduledJobParameter.class,
        // new ParseField("JOB_TYPE"), this.jobParser);
        List<NamedXContentRegistry.Entry> namedXContentRegistryEntries = new ArrayList<>();
        // namedXContentRegistryEntries.add(xContentRegistryEntry);
        this.xContentRegistry = new NamedXContentRegistry(namedXContentRegistryEntries);

        this.settings = Settings.builder().build();

        this.discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);

        Set<Setting<?>> settingSet = new HashSet<>();
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(JobSchedulerSettings.REQUEST_TIMEOUT);
        settingSet.add(JobSchedulerSettings.SWEEP_PERIOD);
        settingSet.add(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingSet.add(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingSet.add(JobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingSet.add(JobSchedulerSettings.JITTER_LIMIT);
        settingSet.add(JobSchedulerSettings.SWEEP_ORPHAN_RECONCILIATION_ENABLED);

        ClusterSettings clusterSettings = new ClusterSettings(this.settings, settingSet);
        ClusterService originClusterService = ClusterServiceUtils.createClusterService(this.threadPool, discoveryNode, clusterSettings);
        this.clusterService = Mockito.spy(originClusterService);

        ScheduledJobProvider jobProvider = new ScheduledJobProvider("JOB_TYPE", "job-index-name", this.jobParser, this.jobRunner);
        Map<String, ScheduledJobProvider> jobProviderMap = new HashMap<>();
        jobProviderMap.put("index-name", jobProvider);

        sweeper = new JobSweeper(
            settings,
            this.client,
            this.clusterService,
            this.threadPool,
            xContentRegistry,
            jobProviderMap,
            scheduler,
            new LockServiceImpl(client, clusterService),
            jobDetailsService
        );
    }

    public void testAfterStart() {
        this.sweeper.afterStart();
        Mockito.verify(this.threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testInitBackgroundSweep() {
        Scheduler.Cancellable cancellable = Mockito.mock(Scheduler.Cancellable.class);
        Mockito.when(this.threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        this.sweeper.initBackgroundSweep();
        Mockito.verify(this.threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString());

        this.sweeper.initBackgroundSweep();
        Mockito.verify(cancellable).cancel();
        Mockito.verify(this.threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testBeforeStop() {
        Scheduler.Cancellable cancellable = Mockito.mock(Scheduler.Cancellable.class);

        this.sweeper.beforeStop();
        Mockito.verify(cancellable, Mockito.times(0)).cancel();

        Mockito.when(this.threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);
        this.sweeper.initBackgroundSweep();
        this.sweeper.beforeStop();
        Mockito.verify(cancellable).cancel();
    }

    public void testBeforeClose() {
        this.sweeper.beforeClose(); // nothing to verify
    }

    public void testPostIndex() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Index index = this.getIndexOperation();
        Engine.IndexResult indexResult = new Engine.IndexResult(1L, 1L, 1L, true);

        Metadata metadata = Metadata.builder().put(createIndexMetadata("index-name", 1, 3)).build();
        RoutingTable routingTable = new RoutingTable.Builder().add(
            new IndexRoutingTable.Builder(metadata.index("index-name").getIndex()).initializeAsNew(metadata.index("index-name")).build()
        ).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster-name"))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        clusterState = this.addNodesToCluter(clusterState, 2);
        clusterState = this.initializeAllShards(clusterState);

        OngoingStubbing stubbing = null;
        Iterator<DiscoveryNode> iter = clusterState.getNodes().iterator();
        while (iter.hasNext()) {
            if (stubbing == null) {
                stubbing = Mockito.when(this.clusterService.localNode()).thenReturn(iter.next());
            } else {
                stubbing = stubbing.thenReturn(iter.next());
            }
        }

        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        JobSweeper testSweeper = Mockito.spy(this.sweeper);
        Mockito.doNothing()
            .when(testSweeper)
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));
        for (int i = 0; i < clusterState.getNodes().getSize(); i++) {
            testSweeper.postIndex(shardId, index, indexResult);
        }

        Mockito.verify(testSweeper)
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));
    }

    public void testPostIndex_indexFailed() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Index index = this.getIndexOperation();
        Engine.IndexResult indexResult = new Engine.IndexResult(new IOException("exception"), 1L);

        this.sweeper.postIndex(shardId, index, indexResult);

        Mockito.verify(this.clusterService, Mockito.times(0)).localNode();
    }

    public void testPostDelete() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Delete delete = this.getDeleteOperation("doc-id");
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(1L, 1L, 1L, true);

        Set<String> jobIdSet = new HashSet<>();
        jobIdSet.add("doc-id");
        Mockito.when(this.scheduler.getScheduledJobIds("index-name")).thenReturn(jobIdSet);

        ActionFuture<DeleteResponse> actionFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(this.client.delete(Mockito.any())).thenReturn(actionFuture);
        DeleteResponse response = new DeleteResponse(new ShardId(new Index("name", "uuid"), 0), "id", 1L, 2L, 3L, true);
        Mockito.when(actionFuture.actionGet()).thenReturn(response);

        this.sweeper.postDelete(shardId, delete, deleteResult);

        Mockito.verify(this.scheduler).deschedule("index-name", "doc-id");
    }

    public void testPostDelete_deletionFailed() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Delete delete = this.getDeleteOperation("doc-id");
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(new IOException("exception"), 1L, 1L);

        this.sweeper.postDelete(shardId, delete, deleteResult);

        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule("index-name", "doc-id");
    }

    public void testSweep() throws IOException {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);

        this.sweeper.sweep(shardId, "id", this.getTestJsonSource(), new JobDocVersion(1L, 1L, 2L));
        Mockito.verify(this.scheduler, Mockito.times(0))
            .schedule(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(JobDocVersion.class),
                Mockito.any(Double.class)
            );

        ScheduledJobParameter mockJobParameter = Mockito.mock(ScheduledJobParameter.class);
        Mockito.when(mockJobParameter.isEnabled()).thenReturn(true);
        Mockito.when(this.jobParser.parse(Mockito.any(), Mockito.anyString(), Mockito.any(JobDocVersion.class)))
            .thenReturn(mockJobParameter);

        this.sweeper.sweep(shardId, "id", this.getTestJsonSource(), new JobDocVersion(1L, 1L, 2L));
        Mockito.verify(this.scheduler)
            .schedule(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(JobDocVersion.class),
                Mockito.any(Double.class)
            );
    }

    public void testSweepUsesSeqNoSort() throws IOException {
        SearchHit hit = new SearchHit(1, "doc-id", null, null);
        hit.sourceRef(this.getTestJsonSource());
        hit.setSeqNo(42L);
        hit.setPrimaryTerm(1L);
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);

        SearchHits emptyHits = new SearchHits(new SearchHit[0], null, 1.0f);

        SearchResponse firstResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(firstResponse.status()).thenReturn(RestStatus.OK);
        Mockito.when(firstResponse.getHits()).thenReturn(hits);

        SearchResponse secondResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(secondResponse.status()).thenReturn(RestStatus.OK);
        Mockito.when(secondResponse.getHits()).thenReturn(emptyHits);

        ActionFuture<SearchResponse> firstFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(firstFuture.actionGet(Mockito.any(TimeValue.class))).thenReturn(firstResponse);

        ActionFuture<SearchResponse> secondFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(secondFuture.actionGet(Mockito.any(TimeValue.class))).thenReturn(secondResponse);

        Mockito.when(this.client.search(Mockito.any())).thenReturn(firstFuture).thenReturn(secondFuture);

        JobSweeper testSweeper = Mockito.spy(this.sweeper);
        Mockito.doNothing()
            .when(testSweeper)
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));

        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);

        testSweeper.sweepIndex("index-name");

        // verify search was called twice: once for the page with the hit, once for the empty page
        Mockito.verify(this.client, Mockito.times(2)).search(Mockito.any());
    }

    public void testSweepAbortsOnNonOkResponse() {
        SearchResponse badResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(badResponse.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);

        ActionFuture<SearchResponse> future = Mockito.mock(ActionFuture.class);
        Mockito.when(future.actionGet(Mockito.any(TimeValue.class))).thenReturn(badResponse);
        Mockito.when(this.client.search(Mockito.any())).thenReturn(future);

        JobSweeper testSweeper = Mockito.spy(this.sweeper);
        Mockito.doNothing()
            .when(testSweeper)
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));

        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);

        testSweeper.sweepIndex("index-name");

        // search was called once, but sweep was never called due to non-OK status
        Mockito.verify(this.client, Mockito.times(1)).search(Mockito.any());
        Mockito.verify(testSweeper, Mockito.times(0))
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));
    }

    public void testSweepAbortsOnSearchException() {
        ActionFuture<SearchResponse> failingFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(failingFuture.actionGet(Mockito.any(TimeValue.class)))
            .thenThrow(new RuntimeException("fielddata access on _id disallowed"));
        Mockito.when(this.client.search(Mockito.any())).thenReturn(failingFuture);

        JobSweeper testSweeper = Mockito.spy(this.sweeper);
        Mockito.doNothing()
            .when(testSweeper)
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));

        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);

        // should not throw — exception is caught and logged
        testSweeper.sweepIndex("index-name");

        // search was attempted once before the exception aborted the loop
        Mockito.verify(this.client, Mockito.times(1)).search(Mockito.any());
        Mockito.verify(testSweeper, Mockito.times(0))
            .sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class), Mockito.any(JobDocVersion.class));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Orphan reconciliation: in-memory jobs whose document was deleted without this node observing the delete
    // (remote-store replicas never execute deletes, so postDelete never fires on the replica-holding node).
    // ---------------------------------------------------------------------------------------------------------------

    public void testReconcileOrphans_deschedulesJobWhoseDocumentIsGone() throws IOException {
        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        seedScheduledJob(shardId, "orphan-job");
        mockEmptyShardSearch();
        mockJobDocumentGet(false);

        this.sweeper.sweepIndex("index-name");

        Mockito.verify(this.client).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler).deschedule("index-name", "orphan-job");
        // the lock of the orphaned job is deleted, like postDelete does
        Mockito.verify(this.client).delete(Mockito.any(DeleteRequest.class), Mockito.any(ActionListener.class));

        // the stale sweep entry is gone: a second sweep has nothing left to reconcile
        this.sweeper.sweepIndex("index-name");
        Mockito.verify(this.client, Mockito.times(1)).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(1)).deschedule("index-name", "orphan-job");
    }

    public void testReconcileOrphans_usesRealtimePrimaryGetWithoutSource() throws IOException {
        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        seedScheduledJob(shardId, "orphan-job");
        mockEmptyShardSearch();
        mockJobDocumentGet(false);

        this.sweeper.sweepIndex("index-name");

        org.mockito.ArgumentCaptor<GetRequest> captor = org.mockito.ArgumentCaptor.forClass(GetRequest.class);
        Mockito.verify(this.client).get(captor.capture());
        GetRequest getRequest = captor.getValue();
        assertEquals("index-name", getRequest.index());
        assertEquals("orphan-job", getRequest.id());
        assertTrue("orphan check must be a realtime GET", getRequest.realtime());
        assertEquals("orphan check must read the primary copy", "_primary", getRequest.preference());
        assertFalse("orphan check must not fetch the source", getRequest.fetchSourceContext().fetchSource());
    }

    public void testReconcileOrphans_keepsJobWhoseDocumentStillExists() throws IOException {
        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        // document was (re-)indexed after the last refresh: not a search hit yet, but the realtime GET sees it
        seedScheduledJob(shardId, "live-job");
        mockEmptyShardSearch();
        mockJobDocumentGet(true);

        this.sweeper.sweepIndex("index-name");

        Mockito.verify(this.client).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(this.client, Mockito.times(0)).delete(Mockito.any(DeleteRequest.class), Mockito.any(ActionListener.class));

        // the entry is kept, so the next sweep checks again
        this.sweeper.sweepIndex("index-name");
        Mockito.verify(this.client, Mockito.times(2)).get(Mockito.any(GetRequest.class));
    }

    public void testReconcileOrphans_leavesJobAloneWhenGetFails() throws IOException {
        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        seedScheduledJob(shardId, "unverified-job");
        mockEmptyShardSearch();
        ActionFuture<GetResponse> failingFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(failingFuture.actionGet(Mockito.any(TimeValue.class))).thenThrow(new RuntimeException("no primary available"));
        Mockito.when(this.client.get(Mockito.any(GetRequest.class))).thenReturn(failingFuture);

        this.sweeper.sweepIndex("index-name");

        Mockito.verify(this.client).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule(Mockito.anyString(), Mockito.anyString());
    }

    public void testReconcileOrphans_dropsStaleEntryOfAlreadyDescheduledJob() throws IOException {
        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        // job was swept once, then postDelete on this node descheduled it: only the sweptJobs entry is left behind
        seedScheduledJob(shardId, "descheduled-job");
        Mockito.when(this.scheduler.getScheduledJobIds("index-name")).thenReturn(new HashSet<>());
        mockEmptyShardSearch();

        this.sweeper.sweepIndex("index-name");

        Mockito.verify(this.client, Mockito.times(0)).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule(Mockito.anyString(), Mockito.anyString());
    }

    public void testReconcileOrphans_ignoresJobsReturnedBySearch() throws IOException {
        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        seedScheduledJob(shardId, "doc-id");
        // the shard search returns the job document (same version as in memory, so sweep() is a no-op)
        SearchHit hit = new SearchHit(1, "doc-id", null, null);
        hit.sourceRef(this.getTestJsonSource());
        hit.setSeqNo(1L);
        hit.setPrimaryTerm(1L);
        hit.version(2L);
        mockShardSearch(new SearchHits(new SearchHit[] { hit }, null, 1.0f));

        this.sweeper.sweepIndex("index-name");

        Mockito.verify(this.client, Mockito.times(0)).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule(Mockito.anyString(), Mockito.anyString());
    }

    public void testReconcileOrphans_disabledBySetting() throws IOException {
        Settings disabled = Settings.builder().put(JobSchedulerSettings.SWEEP_ORPHAN_RECONCILIATION_ENABLED.getKey(), false).build();
        ScheduledJobProvider jobProvider = new ScheduledJobProvider("JOB_TYPE", "job-index-name", this.jobParser, this.jobRunner);
        Map<String, ScheduledJobProvider> jobProviderMap = new HashMap<>();
        jobProviderMap.put("index-name", jobProvider);
        JobSweeper disabledSweeper = new JobSweeper(
            disabled,
            this.client,
            this.clusterService,
            this.threadPool,
            xContentRegistry,
            jobProviderMap,
            scheduler,
            new LockServiceImpl(client, clusterService),
            jobDetailsService
        );

        ClusterState clusterState = buildSingleShardClusterState("index-name");
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId shardId = shardIdOf(clusterState, "index-name", 0);

        seedScheduledJob(disabledSweeper, shardId, "orphan-job");
        mockEmptyShardSearch();
        mockJobDocumentGet(false);

        disabledSweeper.sweepIndex("index-name");

        Mockito.verify(this.client, Mockito.times(0)).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule(Mockito.anyString(), Mockito.anyString());
    }

    public void testReconcileOrphans_skipsJobNotRoutedToShardById() throws IOException {
        // two shards on one node; the job entry is (artificially) recorded on the shard its id does NOT route to,
        // which is what a document indexed with a custom routing value looks like from the sweeper's point of view
        ClusterState clusterState = buildClusterState("index-name", 2);
        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        ShardId routedShard = this.clusterService.operationRouting().shardId(clusterState, "index-name", "custom-routed-job", null);
        ShardId otherShard = shardIdOf(clusterState, "index-name", routedShard.getId() == 0 ? 1 : 0);

        seedScheduledJob(otherShard, "custom-routed-job");
        mockEmptyShardSearch();
        mockJobDocumentGet(false);

        this.sweeper.sweepIndex("index-name");

        // cannot be verified by a GET by id, so it must be left alone
        Mockito.verify(this.client, Mockito.times(0)).get(Mockito.any(GetRequest.class));
        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule(Mockito.anyString(), Mockito.anyString());
    }

    /** Puts a job into the sweeper's in-memory state (sweptJobs + "scheduled" on this node) as a completed sweep would. */
    private void seedScheduledJob(ShardId shardId, String jobId) throws IOException {
        seedScheduledJob(this.sweeper, shardId, jobId);
    }

    private void seedScheduledJob(JobSweeper targetSweeper, ShardId shardId, String jobId) throws IOException {
        ScheduledJobParameter mockJobParameter = Mockito.mock(ScheduledJobParameter.class);
        Mockito.when(mockJobParameter.isEnabled()).thenReturn(true);
        Mockito.when(this.jobParser.parse(Mockito.any(), Mockito.eq(jobId), Mockito.any(JobDocVersion.class))).thenReturn(mockJobParameter);
        targetSweeper.sweep(shardId, jobId, this.getTestJsonSource(), new JobDocVersion(1L, 1L, 2L));

        Set<String> scheduled = new HashSet<>();
        scheduled.add(jobId);
        Mockito.when(this.scheduler.getScheduledJobIds("index-name")).thenReturn(scheduled);
    }

    private void mockEmptyShardSearch() {
        mockShardSearch(new SearchHits(new SearchHit[0], null, 1.0f));
    }

    /** First page returns {@code hits}, every following page is empty (ends the paged traversal). */
    private void mockShardSearch(SearchHits hits) {
        SearchResponse firstResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(firstResponse.status()).thenReturn(RestStatus.OK);
        Mockito.when(firstResponse.getHits()).thenReturn(hits);
        SearchResponse emptyResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(emptyResponse.status()).thenReturn(RestStatus.OK);
        Mockito.when(emptyResponse.getHits()).thenReturn(new SearchHits(new SearchHit[0], null, 1.0f));

        ActionFuture<SearchResponse> firstFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(firstFuture.actionGet(Mockito.any(TimeValue.class))).thenReturn(firstResponse);
        ActionFuture<SearchResponse> emptyFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(emptyFuture.actionGet(Mockito.any(TimeValue.class))).thenReturn(emptyResponse);

        if (hits.getHits().length == 0) {
            Mockito.when(this.client.search(Mockito.any())).thenReturn(emptyFuture);
        } else {
            Mockito.when(this.client.search(Mockito.any())).thenReturn(firstFuture).thenReturn(emptyFuture);
        }
    }

    private void mockJobDocumentGet(boolean exists) {
        GetResponse getResponse = Mockito.mock(GetResponse.class);
        Mockito.when(getResponse.isExists()).thenReturn(exists);
        ActionFuture<GetResponse> getFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(getFuture.actionGet(Mockito.any(TimeValue.class))).thenReturn(getResponse);
        Mockito.when(this.client.get(Mockito.any(GetRequest.class))).thenReturn(getFuture);
    }

    private ShardId shardIdOf(ClusterState clusterState, String indexName, int shard) {
        return clusterState.routingTable().index(indexName).shard(shard).shardId();
    }

    private ClusterState buildSingleShardClusterState(String indexName) {
        return buildClusterState(indexName, 1);
    }

    private ClusterState buildClusterState(String indexName, int numberOfShards) {
        Metadata metadata = Metadata.builder().put(createIndexMetadata(indexName, 0, numberOfShards)).build();
        RoutingTable routingTable = new RoutingTable.Builder().add(
            new IndexRoutingTable.Builder(metadata.index(indexName).getIndex()).initializeAsNew(metadata.index(indexName)).build()
        ).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster-name"))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        clusterState = this.addNodesToCluter(clusterState, 1);
        clusterState = this.initializeAllShards(clusterState);
        // set local node so getLocalShards can match shards assigned to this node
        String firstNodeId = clusterState.getNodes().iterator().next().getId();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.getNodes()).localNodeId(firstNodeId))
            .build();
        return clusterState;
    }

    private ClusterState addNodesToCluter(ClusterState clusterState, int nodeCount) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder();
        for (int i = 1; i <= nodeCount; i++) {
            nodeBuilder.add(OpenSearchAllocationTestCase.newNode("node-" + i));
        }

        return ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    private ClusterState initializeAllShards(ClusterState clusterState) {
        AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE)
                .put("cluster.routing.allocation.node_initial_parimaries_recoveries", Integer.MAX_VALUE)
                .build()
        );
        clusterState = allocationService.reroute(clusterState, "reroute");
        clusterState = allocationService.applyStartedShards(
            clusterState,
            clusterState.getRoutingNodes().shardsWithState("index-name", ShardRoutingState.INITIALIZING)
        ); // start primary shard
        clusterState = allocationService.applyStartedShards(
            clusterState,
            clusterState.getRoutingNodes().shardsWithState("index-name", ShardRoutingState.INITIALIZING)
        ); // start replica shards
        return clusterState;
    }

    private Engine.Index getIndexOperation() {
        String docId = "doc-id";
        long primaryTerm = 1L;
        List<ParseContext.Document> docs = new ArrayList<>();
        docs.add(new ParseContext.Document());
        BytesReference source = this.getTestJsonSource();

        Term uid = new Term(
            "id_field",
            new BytesRef(docId.getBytes(Charset.defaultCharset()), 0, docId.getBytes(Charset.defaultCharset()).length)
        );
        ParsedDocument parsedDocument = new ParsedDocument(null, null, docId, null, docs, source, null, null);

        return new Engine.Index(uid, primaryTerm, parsedDocument);
    }

    private Engine.Delete getDeleteOperation(String docId) {
        Term uid = new Term(
            "id_field",
            new BytesRef(docId.getBytes(Charset.defaultCharset()), 0, docId.getBytes(Charset.defaultCharset()).length)
        );
        return new Engine.Delete(docId, uid, 1L);
    }

    private BytesReference getTestJsonSource() {
        return new BytesArray(
            "{\n"
                + "\t\"id\": \"id\",\n"
                + "\t\"name\": \"name\",\n"
                + "\t\"version\": 3,\n"
                + "\t\"enabled\": true,\n"
                + "\t\"schedule\": {\n"
                + "\t\t\"cron\": {\n"
                + "\t\t\t\"expression\": \"* * * * *\",\n"
                + "\t\t\t\"timezone\": \"PST8PDT\"\n"
                + "\t\t}\n"
                + "\t},\n"
                + "\t\"sample_param\": \"sample parameter\",\n"
                + "\t\"enable_time\": 1550105987448,\n"
                + "\t\"last_update_time\": 1550105987448\n"
                + "}"
        );
    }

    private IndexMetadata.Builder createIndexMetadata(String indexName, int replicaNumber, int shardNumber) {
        Settings defaultSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return new IndexMetadata.Builder(indexName).settings(defaultSettings).numberOfReplicas(replicaNumber).numberOfShards(shardNumber);
    }
}
