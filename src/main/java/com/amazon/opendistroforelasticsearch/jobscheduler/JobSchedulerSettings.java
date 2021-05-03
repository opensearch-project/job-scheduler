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

package com.amazon.opendistroforelasticsearch.jobscheduler;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

public class JobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "opensearch.jobscheduler.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "opensearch.jobscheduler.sweeper.backoff_millis",
            TimeValue.timeValueMillis(50),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "opensearch.jobscheduler.retry_count",
            3, 
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
            "opensearch.jobscheduler.sweeper.period",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
            "opensearch.jobscheduler.sweeper.page_size",
            100, 
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Double> JITTER_LIMIT = Setting.doubleSetting(
            "opensearch.jobscheduler.jitter_limit",
            0.60, 0, 0.95, 
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    // legacy settings from OpenDistro
    
    public static final Setting<TimeValue> LEGACY_OPENDISTRO_REQUEST_TIMEOUT = REQUEST_TIMEOUT.withKey(
            "opendistro.jobscheduler.request_timeout");

    public static final Setting<TimeValue> LEGACY_OPENDISTRO_SWEEP_BACKOFF_MILLIS = SWEEP_BACKOFF_MILLIS.withKey(
            "opendistro.jobscheduler.sweeper.backoff_millis");

    public static final Setting<Integer> LEGACY_OPENDISTRO_SWEEP_BACKOFF_RETRY_COUNT = SWEEP_BACKOFF_RETRY_COUNT.withKey(
            "opendistro.jobscheduler.retry_count");

    public static final Setting<TimeValue> LEGACY_OPENDISTRO_SWEEP_PERIOD = SWEEP_PERIOD.withKey(
            "opendistro.jobscheduler.sweeper.period");

    public static final Setting<Integer> LEGACY_OPENDISTRO_SWEEP_PAGE_SIZE = SWEEP_PAGE_SIZE.withKey(
            "opendistro.jobscheduler.sweeper.page_size");

    public static final Setting<Double> LEGACY_OPENDISTRO_JITTER_LIMIT = JITTER_LIMIT.withKey(
            "opendistro.jobscheduler.jitter_limit");
}
