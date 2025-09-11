/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.remote.metadata.common.CommonValue.AWS_DYNAMO_DB;

@SuppressWarnings({ "rawtypes" })
public class JobSchedulerSettingsTests extends OpenSearchTestCase {

    JobSchedulerPlugin plugin;

    @Before
    public void setup() {
        this.plugin = new JobSchedulerPlugin();
    }

    public void testAllLegacyOpenDistroSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
            "legacy setting must be returned from settings",
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
        assertTrue(
            "legacy setting must be returned from settings",
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

        assertSettingDeprecationsAndWarnings(
            new Setting[] {
                LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
                LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
                LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
                LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD,
                LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
                LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT }
        );
    }

    public void testRemoteMetadataSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
            "remote metadata settings must be returned from settings",
            settings.containsAll(
                Arrays.asList(
                    JobSchedulerSettings.REMOTE_METADATA_TYPE,
                    JobSchedulerSettings.REMOTE_METADATA_ENDPOINT,
                    JobSchedulerSettings.REMOTE_METADATA_REGION,
                    JobSchedulerSettings.REMOTE_METADATA_SERVICE_NAME,
                    JobSchedulerSettings.JOB_SCHEDULER_MULTI_TENANCY_ENABLED
                )
            )
        );
    }

    public void testRemoteMetadataSettingsDefaults() {
        Settings settings = Settings.EMPTY;

        assertEquals("", JobSchedulerSettings.REMOTE_METADATA_TYPE.get(settings));
        assertEquals("", JobSchedulerSettings.REMOTE_METADATA_ENDPOINT.get(settings));
        assertEquals("", JobSchedulerSettings.REMOTE_METADATA_REGION.get(settings));
        assertEquals("", JobSchedulerSettings.REMOTE_METADATA_SERVICE_NAME.get(settings));
        assertFalse(JobSchedulerSettings.JOB_SCHEDULER_MULTI_TENANCY_ENABLED.get(settings));
    }

    public void testRemoteMetadataSettingsValues() {
        Settings settings = Settings.builder()
            .put("plugins.jobscheduler.remote_metadata_type", AWS_DYNAMO_DB)
            .put("plugins.jobscheduler.remote_metadata_endpoint", "https://dynamodb.us-east-1.amazonaws.com")
            .put("plugins.jobscheduler.remote_metadata_region", "us-east-1")
            .put("plugins.jobscheduler.remote_metadata_service_name", "es")
            .put("plugins.jobscheduler.tenant_aware", true)
            .build();

        assertEquals(AWS_DYNAMO_DB, JobSchedulerSettings.REMOTE_METADATA_TYPE.get(settings));
        assertEquals("https://dynamodb.us-east-1.amazonaws.com", JobSchedulerSettings.REMOTE_METADATA_ENDPOINT.get(settings));
        assertEquals("us-east-1", JobSchedulerSettings.REMOTE_METADATA_REGION.get(settings));
        assertEquals("es", JobSchedulerSettings.REMOTE_METADATA_SERVICE_NAME.get(settings));
        assertTrue(JobSchedulerSettings.JOB_SCHEDULER_MULTI_TENANCY_ENABLED.get(settings));
    }
}
