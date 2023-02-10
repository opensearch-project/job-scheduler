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
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.TestHelpers;
import org.opensearch.jobscheduler.rest.action.RestReleaseLockAction;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestReleaseLockActionIT extends OpenSearchTestCase {

    private RestReleaseLockAction restReleaseLockAction;

    private LockService lockService;

    private String releaseLockPath;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        lockService = new LockService(Mockito.mock(Client.class), Mockito.mock(ClusterService.class));
        restReleaseLockAction = new RestReleaseLockAction(lockService);
        this.releaseLockPath = String.format(
            Locale.ROOT,
            "%s/%s/{%s}",
            JobSchedulerPlugin.JS_BASE_URI,
            "_release_lock",
            restReleaseLockAction.LOCK_ID
        );

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
        params.put("lock_id", "lock_id");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(releaseLockPath)
            .withParams(params)
            .build();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
        Mockito.doNothing().when(lockService).findLock("lock_id", ActionListener.wrap(response -> {}, exception -> {}));
        Mockito.doNothing().when(lockService).release(TestHelpers.randomLockModel(), ActionListener.wrap(response -> {}, exception -> {}));
        restReleaseLockAction.prepareRequest(request, Mockito.mock(NodeClient.class));
        assertEquals(channel.responses().get(), 0);
        assertEquals(channel.errors().get(), 0);
    }
}
