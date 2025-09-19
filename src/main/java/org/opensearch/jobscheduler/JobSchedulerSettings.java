/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_AWARE_KEY;

public class JobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
        "plugins.jobscheduler.request_timeout",
        LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
        "plugins.jobscheduler.sweeper.backoff_millis",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
        "plugins.jobscheduler.retry_count",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
        "plugins.jobscheduler.sweeper.period",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
        "plugins.jobscheduler.sweeper.page_size",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Double> JITTER_LIMIT = Setting.doubleSetting(
        "plugins.jobscheduler.jitter_limit",
        LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> STATUS_HISTORY = Setting.boolSetting(
        "plugins.jobscheduler.history.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets the remote metadata type */
    public static final Setting<String> REMOTE_METADATA_TYPE = Setting.simpleString(
        "plugins.jobscheduler." + REMOTE_METADATA_TYPE_KEY,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /** This setting sets the remote metadata endpoint */
    public static final Setting<String> REMOTE_METADATA_ENDPOINT = Setting.simpleString(
        "plugins.jobscheduler." + REMOTE_METADATA_ENDPOINT_KEY,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /** This setting sets the remote metadata region */
    public static final Setting<String> REMOTE_METADATA_REGION = Setting.simpleString(
        "plugins.jobscheduler." + REMOTE_METADATA_REGION_KEY,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /** This setting sets the remote metadata service name */
    public static final Setting<String> REMOTE_METADATA_SERVICE_NAME = Setting.simpleString(
        "plugins.jobscheduler." + REMOTE_METADATA_SERVICE_NAME_KEY,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /** This setting enables multi-tenancy for job scheduler */
    public static final Setting<Boolean> JOB_SCHEDULER_MULTI_TENANCY_ENABLED = Setting.boolSetting(
        "plugins.jobscheduler." + TENANT_AWARE_KEY,
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
}
