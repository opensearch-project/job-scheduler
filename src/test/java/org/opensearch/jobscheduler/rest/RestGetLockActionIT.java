/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.AcquireLockRequest;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestGetLockActionIT extends OpenSearchTestCase {

    private ClusterService clusterService;
    private LockService lockService;
    private RestGetLockAction getLockAction;
    private String getLockPath;
    private String testJobId;
    private String testJobIndexName;
    private long testLockDurationSeconds;
    private String requestBody;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.clusterService.state().routingTable().hasIndex(".opendistro-job-scheduler-lock")).thenReturn(true);
        this.lockService = new LockService(Mockito.mock(NodeClient.class), clusterService);
        this.getLockAction = new RestGetLockAction(this.lockService);
        this.getLockPath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_lock");
        this.testJobId = "testJobId";
        this.testJobIndexName = "testJobIndexName";
        this.testLockDurationSeconds = 1L;
        this.requestBody = "{\"job_id\":\""
            + this.testJobId
            + "\",\"job_index_name\":\""
            + this.testJobIndexName
            + "\",\"lock_duration_seconds\":\""
            + this.testLockDurationSeconds
            + "\"}";
    }

    public void testGetNames() {
        String name = getLockAction.getName();
        assertEquals(RestGetLockAction.GET_LOCK_ACTION, name);
    }

    public void testGetRoutes() {
        List<RestHandler.Route> routes = getLockAction.routes();
        assertEquals(getLockPath, routes.get(0).getPath());
    }

    public void testAcquireLockRequest() throws IOException {

        // Create AcquireLockRequest
        AcquireLockRequest acquireLockRequest = new AcquireLockRequest(testJobId, testJobIndexName, testLockDurationSeconds);

        // Generate Xcontent from request
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(AcquireLockRequest.JOB_ID, acquireLockRequest.getJobId());
        builder.field(AcquireLockRequest.JOB_INDEX_NAME, acquireLockRequest.getJobIndexName());
        builder.field(AcquireLockRequest.LOCK_DURATION_SECONDS, acquireLockRequest.getLockDurationSeconds());
        builder.endObject();

        // Test request serde logic
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, TestHelpers.xContentBuilderToString(builder));
        parser.nextToken();
        acquireLockRequest = AcquireLockRequest.parse(parser);
        assertEquals(this.testJobId, acquireLockRequest.getJobId());
        assertEquals(this.testJobIndexName, acquireLockRequest.getJobIndexName());
        assertEquals(this.testLockDurationSeconds, acquireLockRequest.getLockDurationSeconds());
    }

    public void testPrepareGetLockRequest() throws IOException {

        // Prepare rest request
        Map<String, String> params = new HashMap<>();
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(this.getLockPath)
            .withParams(params)
            .withContent(new BytesArray(this.requestBody), XContentType.JSON)
            .build();

        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        this.getLockAction.prepareRequest(request, Mockito.mock(NodeClient.class));
        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }

}
