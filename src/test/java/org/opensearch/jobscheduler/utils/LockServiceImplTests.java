/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.mockito.Mockito;
import org.opensearch.Version;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

public class LockServiceImplTests extends OpenSearchTestCase {
    public void testRefreshPolicyFollowsLockIndexMetadata() {
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        LockServiceImpl service = new LockServiceImpl(Mockito.mock(Client.class), clusterService);

        Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, service.lockRefreshPolicy());

        // Recheck current metadata on each write, including after a remote-store migration.
        for (boolean remoteStore : new boolean[] { false, true, false }) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, remoteStore);
            if (remoteStore) {
                settings.put("index.replication.type", "SEGMENT")
                    .put("index.remote_store.segment.repository", "segments")
                    .put("index.remote_store.translog.repository", "translog");
            }
            IndexMetadata index = IndexMetadata.builder(LockServiceImpl.LOCK_INDEX_NAME)
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            Mockito.when(clusterService.state())
                .thenReturn(ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(index, false)).build());
            assertEquals(remoteStore ? WriteRequest.RefreshPolicy.NONE : WriteRequest.RefreshPolicy.IMMEDIATE, service.lockRefreshPolicy());
        }
    }
}
