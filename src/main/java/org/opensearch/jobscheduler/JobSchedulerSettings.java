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
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.common.unit.TimeValue;

public class JobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
        "plugins.jobscheduler.request_timeout",
        LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Timeout for waiting on sweeper searches and checks for deleted job documents; does not limit job execution time.");

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
        "plugins.jobscheduler.sweeper.backoff_millis",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Initial delay for exponential backoff when retrying failed sweeper searches.");

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
        "plugins.jobscheduler.retry_count",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Maximum number of retries for failed sweeper searches.");

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
        "plugins.jobscheduler.sweeper.period",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Interval between background full sweeps that discover and reconcile scheduled jobs.");

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
        "plugins.jobscheduler.sweeper.page_size",
        LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Maximum number of job documents fetched per page during a sweep.");

    public static final Setting<Double> JITTER_LIMIT = Setting.doubleSetting(
        "plugins.jobscheduler.jitter_limit",
        LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Caps the jitter requested by a job as a fraction of its scheduling interval. Jobs without jitter are not delayed.");

    public static final Setting<Boolean> STATUS_HISTORY = Setting.boolSetting(
        "plugins.jobscheduler.history.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    ).withDescription("Records job execution history when job locks are acquired and released.");

    /**
     * When enabled, the full sweep deschedules in-memory jobs whose backing document no longer exists in the job
     * index (confirmed with a realtime GET against the primary shard). This repairs jobs that were orphaned because
     * the delete of their document was never observed on this node: on remote-store enabled job indices replicas do
     * not execute delete operations, so the postDelete listener never fires on the replica-holding node.
     *
     * Unless set explicitly, the setting follows the node's remote store configuration: it is enabled on nodes that
     * carry a remote segment or translog repository attribute (where the listener gap exists) and disabled on all
     * other nodes. An explicit value, including a dynamic cluster settings update, always takes precedence; removing
     * the cluster setting again restores the derived default.
     */
    public static final Setting<Boolean> SWEEP_ORPHAN_RECONCILIATION_ENABLED = Setting.boolSetting(
        "plugins.jobscheduler.sweeper.orphan_reconciliation.enabled",
        settings -> Boolean.toString(RemoteStoreNodeAttribute.isRemoteDataAttributePresent(settings)),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    )
        .withDescription(
            "Removes scheduled jobs whose backing documents were deleted. "
                + "When unset, enabled on nodes with remote segment or translog repository attributes."
        );
}
