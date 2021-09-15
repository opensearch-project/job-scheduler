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

package org.opensearch.jobscheduler.spi.schedule;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.cronutils.utils.VisibleForTesting;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

/**
 * UnixCron {@link Schedule} implementation. Refer to https://en.wikipedia.org/wiki/Cron for cron syntax.
 */
public class CronSchedule implements Schedule {
    static final String CRON_FIELD = "cron";
    static final String EXPRESSION_FIELD = "expression";
    static final String TIMEZONE_FIELD = "timezone";

    private static CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));

    private ZoneId timezone;
    private String expression;
    private ExecutionTime executionTime;
    private Clock clock;
    private Long scheduleDelay;

    public CronSchedule(String expression, ZoneId timezone) {
        this.expression = expression;
        this.timezone = timezone;
        this.executionTime = ExecutionTime.forCron(cronParser.parse(this.expression));
        clock = Clock.system(timezone);
    }

    public CronSchedule(String expression, ZoneId timezone, long scheduleDelay) {
        this(expression, timezone);
        this.scheduleDelay = scheduleDelay;
    }

    public CronSchedule(StreamInput input) throws IOException {
        timezone = input.readZoneId();
        expression = input.readString();
        scheduleDelay = input.readOptionalLong();
        executionTime = ExecutionTime.forCron(cronParser.parse(expression));
        clock = Clock.system(timezone);
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    @VisibleForTesting
    void setExecutionTime(ExecutionTime executionTime) {
        this.executionTime = executionTime;
    }

    @VisibleForTesting
    ZoneId getTimeZone() {
        return this.timezone;
    }

    @VisibleForTesting
    String getCronExpression() {
        return this.expression;
    }

    public long getDelay() { return this.scheduleDelay == null ? 0 : this.scheduleDelay; }

    public void setDelay(long delay) { this.scheduleDelay = delay; }

    @Override
    public Instant getNextExecutionTime(Instant time) {
        Instant baseTime = time == null ? this.clock.instant() : time;
        long delay = scheduleDelay == null ? 0 : scheduleDelay;

        // the executionTime object doesn't use the delay, need to remove the delay before and then add it back after
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(baseTime.minusMillis(delay), this.timezone);
        ZonedDateTime nextExecutionTime = this.executionTime.nextExecution(zonedDateTime).orElse(null);

        return nextExecutionTime == null ? null : nextExecutionTime.toInstant().plusMillis(delay);
    }

    @Override
    public Duration nextTimeToExecute() {
        long delay = scheduleDelay == null ? 0 : scheduleDelay;
        Instant now = this.clock.instant().minusMillis(delay);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(now, this.timezone);
        Optional<Duration> timeToNextExecution = this.executionTime.timeToNextExecution(zonedDateTime);
        return timeToNextExecution.orElse(null);
    }

    @Override
    public Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime) {
        long delay = scheduleDelay == null ? 0 : scheduleDelay;
        Instant realStartTime;
        if (startTime != null) {
            realStartTime = startTime;
        } else {
            Instant now = this.clock.instant();
            Optional<ZonedDateTime> lastExecutionTime = this.executionTime.lastExecution(ZonedDateTime.ofInstant(now.minusMillis(delay), this.timezone));
            if (!lastExecutionTime.isPresent()) {
                return new Tuple<>(now, now);
            }
            realStartTime = lastExecutionTime.get().toInstant().plusMillis(delay);
        }
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(realStartTime.minusMillis(delay), this.timezone);
        ZonedDateTime newEndTime = executionTime.nextExecution(zonedDateTime).orElse(null);
        return new Tuple<>(realStartTime, newEndTime == null ? null : newEndTime.toInstant().plusMillis(delay));
    }

    @Override
    public Boolean runningOnTime(Instant lastExecutionTime) {
        long delay = scheduleDelay == null ? 0 : scheduleDelay;
        if (lastExecutionTime == null) {
            return true;
        }

        Instant now = this.clock.instant().minusMillis(delay);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(now, timezone);
        Optional<ZonedDateTime> expectedExecutionTime = this.executionTime.lastExecution(zonedDateTime);

        if (!expectedExecutionTime.isPresent()) {
            return false;
        }
        ZonedDateTime actualExecutionTime = ZonedDateTime.ofInstant(lastExecutionTime, timezone);

        return ChronoUnit.SECONDS.between(expectedExecutionTime.get().plus(delay, ChronoUnit.MILLIS), actualExecutionTime) == 0L;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .startObject(CRON_FIELD)
                .field(EXPRESSION_FIELD, this.expression)
                .field(TIMEZONE_FIELD, this.timezone.getId());
        if (this.scheduleDelay != null) { builder.field(DELAY_FIELD, this.scheduleDelay); }
                builder.endObject()
                .endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, false, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CronSchedule cronSchedule = (CronSchedule) o;
        return timezone.equals(cronSchedule.timezone) &&
                expression.equals(cronSchedule.expression) &&
                Objects.equals(scheduleDelay, cronSchedule.scheduleDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timezone, expression, scheduleDelay);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZoneId(timezone);
        out.writeString(expression);
        out.writeOptionalLong(scheduleDelay);
    }
}
