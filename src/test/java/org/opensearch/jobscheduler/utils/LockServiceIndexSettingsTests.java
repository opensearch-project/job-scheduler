/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.opensearch.test.OpenSearchTestCase;

public class LockServiceIndexSettingsTests extends OpenSearchTestCase {

    public void testLockIndexUsesDeploymentDefaultPrimaryShardCount() {
        assertFalse(LockServiceImpl.INDEX_SETTINGS.containsKey("index.number_of_shards"));
        assertEquals("0-1", LockServiceImpl.INDEX_SETTINGS.get("index.auto_expand_replicas"));
    }

    public void testLockIndexConfiguresPeriodicFlush() {
        assertEquals("10m", LockServiceImpl.INDEX_SETTINGS.get("index.periodic_flush_interval"));
    }
}
