/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.jobscheduler;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings({"rawtypes"})
public class JobSchedulerSettingsTests extends OpenSearchTestCase {

    JobSchedulerPlugin plugin;
    
    @Before
    public void setup() {
        this.plugin = new JobSchedulerPlugin();
    }

    public void testAllLegacyOpenDistroSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue("legacy setting must be returned from settings", 
            settings.containsAll(
                Arrays.asList(
                    LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT,
                    LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
                    LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
                    LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
                    LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
                    LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD
                )
            )
        );
    }

    public void testAllOpenSearchSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue("legacy setting must be returned from settings", 
            settings.containsAll(
                Arrays.asList(
                    JobSchedulerSettings.JITTER_LIMIT,
                    JobSchedulerSettings.REQUEST_TIMEOUT,
                    JobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
                    JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
                    JobSchedulerSettings.SWEEP_PAGE_SIZE,
                    JobSchedulerSettings.SWEEP_PERIOD
                )
            )
        );
    }

    public void testLegacyOpenDistroSettingsFallback() {
        assertEquals(
            JobSchedulerSettings.REQUEST_TIMEOUT.get(Settings.EMPTY), 
            LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT.get(Settings.EMPTY)
        );
    }

    public void testSettingsGetValue() {
        Settings settings = Settings.builder().put("plugins.jobscheduler.request_timeout", "42s").build();
        assertEquals(JobSchedulerSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(42)); 
        assertEquals(LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(10)); 
    }

    public void testSettingsGetValueWithLegacyFallback() {
        Settings settings = Settings.builder()
            .put("opendistro.jobscheduler.request_timeout", "1s")
            .put("opendistro.jobscheduler.sweeper.backoff_millis", "2ms")
            .put("opendistro.jobscheduler.retry_count", 3)
            .put("opendistro.jobscheduler.sweeper.period", "4s")
            .put("opendistro.jobscheduler.sweeper.page_size", 5)
            .put("opendistro.jobscheduler.jitter_limit", 6)
        .build();
        
        assertEquals(JobSchedulerSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(1)); 
        assertEquals(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(2)); 
        assertEquals(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT.get(settings), Integer.valueOf(3)); 
        assertEquals(JobSchedulerSettings.SWEEP_PERIOD.get(settings), TimeValue.timeValueSeconds(4)); 
        assertEquals(JobSchedulerSettings.SWEEP_PAGE_SIZE.get(settings), Integer.valueOf(5)); 
        assertEquals(JobSchedulerSettings.JITTER_LIMIT.get(settings), Double.valueOf(6.0)); 

        assertSettingDeprecationsAndWarnings(new Setting[]{
            LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
            LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
            LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
            LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD,
            LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
            LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT
        });
    }
}
