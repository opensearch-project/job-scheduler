/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.Client;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestReleaseLockActionTests extends OpenSearchTestCase {

    private RestReleaseLockAction restReleaseLockAction;

    private LockService lockService;

    private String releaseLockPath;

    private ClusterService clusterService;

    private Client client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        this.client = Mockito.mock(Client.class);
        this.lockService = new LockService(client, clusterService);
        restReleaseLockAction = new RestReleaseLockAction(this.lockService);
        this.releaseLockPath = String.format(Locale.ROOT, "%s/%s/{%s}", JobSchedulerPlugin.JS_BASE_URI, "_release_lock", LockModel.LOCK_ID);

    }

    public void testGetNames() {
        String name = restReleaseLockAction.getName();
        assertEquals(restReleaseLockAction.RELEASE_LOCK_ACTION, name);
    }

    public void testGetRoutes() {
        List<RestHandler.Route> routes = restReleaseLockAction.routes();
        assertEquals(releaseLockPath, routes.get(0).getPath());
    }

    public void testPrepareReleaseLockRequest() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(LockModel.LOCK_ID, "lock_id");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(releaseLockPath)
            .withParams(params)
            .build();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }
}
