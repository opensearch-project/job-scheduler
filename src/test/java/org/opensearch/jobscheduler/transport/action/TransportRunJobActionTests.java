/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.action;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.scheduler.JobSchedulingInfo;
import org.opensearch.jobscheduler.scheduler.ScheduledJobInfo;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.request.RunJobNodeRequest;
import org.opensearch.jobscheduler.transport.request.RunJobRequest;
import org.opensearch.jobscheduler.transport.response.RunJobNodeResponse;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class TransportRunJobActionTests extends OpenSearchTestCase {

    private TransportRunJobAction action;
    private JobScheduler jobScheduler;
    private JobDetailsService jobDetailsService;
    private LockService lockService;
    private DiscoveryNode localNode;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        ThreadPool threadPool = Mockito.mock(ThreadPool.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        TransportService transportService = Mockito.mock(TransportService.class, Mockito.RETURNS_DEEP_STUBS);
        ActionFilters actionFilters = new ActionFilters(new HashSet<>());

        jobScheduler = Mockito.mock(JobScheduler.class);
        jobDetailsService = Mockito.mock(JobDetailsService.class);
        lockService = Mockito.mock(LockService.class);

        localNode = new DiscoveryNode("node1", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        when(clusterService.localNode()).thenReturn(localNode);

        action = new TransportRunJobAction(
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            jobScheduler,
            jobDetailsService,
            lockService
        );
    }

    public void testNodeOperation_unknownJobType_returnsNoProviderMessage() {
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(Collections.emptyMap());

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("unknown-type", "job-1"));
        RunJobNodeResponse response = action.nodeOperation(request);

        assertFalse(response.isExecuted());
        assertTrue(response.getMessage().contains("no provider registered for job_type [unknown-type]"));
    }

    public void testNodeOperation_knownJobTypeWithDifferentProvider_returnNoProviderMessage() {
        ScheduledJobProvider provider = new ScheduledJobProvider("other-type", "other-index", null, null);
        Map<String, ScheduledJobProvider> providers = new HashMap<>();
        providers.put("other-index", provider);
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(providers);

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("my-job-type", "job-1"));
        RunJobNodeResponse response = action.nodeOperation(request);

        assertFalse(response.isExecuted());
        assertTrue(response.getMessage().contains("no provider registered for job_type [my-job-type]"));
    }

    public void testNodeOperation_jobNotOnThisNode_returnNotScheduledMessage() {
        ScheduledJobProvider provider = new ScheduledJobProvider("my-type", "my-index", null, null);
        Map<String, ScheduledJobProvider> providers = new HashMap<>();
        providers.put("my-index", provider);
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(providers);

        ScheduledJobInfo scheduledJobInfo = Mockito.mock(ScheduledJobInfo.class);
        when(jobScheduler.getScheduledJobInfo()).thenReturn(scheduledJobInfo);
        when(scheduledJobInfo.getJobInfo("my-index", "job-1")).thenReturn(null);

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("my-type", "job-1"));
        RunJobNodeResponse response = action.nodeOperation(request);

        assertFalse(response.isExecuted());
        assertEquals("job not scheduled on this node", response.getMessage());
    }

    public void testNodeOperation_nullRunner_returnNoRunnerMessage() {
        ScheduledJobProvider provider = new ScheduledJobProvider("my-type", "my-index", null, null);
        Map<String, ScheduledJobProvider> providers = new HashMap<>();
        providers.put("my-index", provider);
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(providers);

        ScheduledJobParameter jobParameter = Mockito.mock(ScheduledJobParameter.class);
        JobSchedulingInfo schedulingInfo = new JobSchedulingInfo("my-index", "job-1", jobParameter);

        ScheduledJobInfo scheduledJobInfo = Mockito.mock(ScheduledJobInfo.class);
        when(jobScheduler.getScheduledJobInfo()).thenReturn(scheduledJobInfo);
        when(scheduledJobInfo.getJobInfo("my-index", "job-1")).thenReturn(schedulingInfo);

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("my-type", "job-1"));
        RunJobNodeResponse response = action.nodeOperation(request);

        assertFalse(response.isExecuted());
        assertTrue(response.getMessage().contains("no runner registered for job_type [my-type]"));
    }

    public void testNodeOperation_successfulExecution_returnExecutedTrue() {
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);
        ScheduledJobProvider provider = new ScheduledJobProvider("my-type", "my-index", null, runner);
        Map<String, ScheduledJobProvider> providers = new HashMap<>();
        providers.put("my-index", provider);
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(providers);

        ScheduledJobParameter jobParameter = Mockito.mock(ScheduledJobParameter.class);
        JobSchedulingInfo schedulingInfo = new JobSchedulingInfo("my-index", "job-1", jobParameter);

        ScheduledJobInfo scheduledJobInfo = Mockito.mock(ScheduledJobInfo.class);
        when(jobScheduler.getScheduledJobInfo()).thenReturn(scheduledJobInfo);
        when(scheduledJobInfo.getJobInfo("my-index", "job-1")).thenReturn(schedulingInfo);

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("my-type", "job-1"));
        RunJobNodeResponse response = action.nodeOperation(request);

        assertTrue(response.isExecuted());
        assertNull(response.getMessage());
        assertEquals("node1", response.getNode().getId());
    }

    public void testNodeOperation_runnerThrowsException_returnExecutionFailedMessage() {
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);
        ScheduledJobProvider provider = new ScheduledJobProvider("my-type", "my-index", null, runner);
        Map<String, ScheduledJobProvider> providers = new HashMap<>();
        providers.put("my-index", provider);
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(providers);

        ScheduledJobParameter jobParameter = Mockito.mock(ScheduledJobParameter.class);
        JobSchedulingInfo schedulingInfo = new JobSchedulingInfo("my-index", "job-1", jobParameter);

        ScheduledJobInfo scheduledJobInfo = Mockito.mock(ScheduledJobInfo.class);
        when(jobScheduler.getScheduledJobInfo()).thenReturn(scheduledJobInfo);
        when(scheduledJobInfo.getJobInfo("my-index", "job-1")).thenReturn(schedulingInfo);

        doThrow(new RuntimeException("lock acquisition failed")).when(runner).runJob(Mockito.any(), Mockito.any());

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("my-type", "job-1"));
        RunJobNodeResponse response = action.nodeOperation(request);

        assertFalse(response.isExecuted());
        assertNotNull(response.getMessage());
        assertTrue(response.getMessage().startsWith("execution failed:"));
        assertTrue(response.getMessage().contains("lock acquisition failed"));
    }

    public void testNodeOperation_multipleProviders_resolvesCorrectProvider() {
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);
        ScheduledJobProvider providerA = new ScheduledJobProvider("type-A", "index-A", null, runner);
        ScheduledJobProvider providerB = new ScheduledJobProvider("type-B", "index-B", null, null);
        Map<String, ScheduledJobProvider> providers = new HashMap<>();
        providers.put("index-A", providerA);
        providers.put("index-B", providerB);
        when(jobDetailsService.getIndexToJobProviders()).thenReturn(providers);

        ScheduledJobParameter jobParameter = Mockito.mock(ScheduledJobParameter.class);
        JobSchedulingInfo schedulingInfo = new JobSchedulingInfo("index-A", "job-99", jobParameter);

        ScheduledJobInfo scheduledJobInfo = Mockito.mock(ScheduledJobInfo.class);
        when(jobScheduler.getScheduledJobInfo()).thenReturn(scheduledJobInfo);
        when(scheduledJobInfo.getJobInfo("index-A", "job-99")).thenReturn(schedulingInfo);

        RunJobNodeRequest request = new RunJobNodeRequest(new RunJobRequest("type-A", "job-99"));
        RunJobNodeResponse response = action.nodeOperation(request);

        // type-A has a valid runner, so execution should succeed
        assertTrue(response.isExecuted());
    }
}
