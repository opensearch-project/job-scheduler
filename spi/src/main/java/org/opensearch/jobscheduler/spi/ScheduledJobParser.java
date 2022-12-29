/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;

public interface ScheduledJobParser {
    ScheduledJobParameter parse(XContentParser xContentParser, String id, JobDocVersion jobDocVersion) throws IOException;
}
