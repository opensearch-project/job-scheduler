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

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

public class JobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.jobscheduler.request_timeout",
            LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "plugins.jobscheduler.sweeper.backoff_millis",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "plugins.jobscheduler.retry_count",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
            "plugins.jobscheduler.sweeper.period",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
            "plugins.jobscheduler.sweeper.page_size",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Double> JITTER_LIMIT = Setting.doubleSetting(
            "plugins.jobscheduler.jitter_limit",
            LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
}
