/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi.schedule;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;

import java.time.Duration;
import java.time.Instant;

public interface Schedule extends Writeable, ToXContentObject {
    static final String DELAY_FIELD = "schedule_delay";

    /**
     * Gets next job execution time of given time parameter.
     *
     * @param time base time point
     * @return next execution time since time parameter.
     */
    Instant getNextExecutionTime(Instant time);

    /**
     * Calculates the time duration between next execution time and now.
     *
     * @return time duration between next execution and now.
     */
    Duration nextTimeToExecute();

    /**
     * Gets the execution period starting at {@code startTime}.
     *
     * @param startTime start time of the period.
     * @return the start time and end time of the period in the tuple.
     */
    Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime);

    /**
     * Returns if the job is running on time.
     *
     * @param lastExecutionTime last execution time.
     * @return true if the job executes on time, otherwise false.
     */
    Boolean runningOnTime(Instant lastExecutionTime);

    /**
     * Gets the delay parameter of the schedule.
     *
     * @return the delay parameter of the schedule as a Long.
     */
    Long getDelay();

}
