/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class StatusHistoryModel implements ToXContentObject, Writeable {
    public static final String JOB_INDEX_NAME = "job_index_name";
    public static final String JOB_ID = "job_id";
    public static final String START_TIME = "start_time";
    public static final String END_TIME = "end_time";
    public static final String COMPLETION_STATUS = "completion_status";

    private final String jobIndexName;
    private final String jobId;
    private final Instant startTime;
    private final Instant endTime;
    private final int status;

    public StatusHistoryModel(String jobIndexName, String jobId, Instant startTime, Instant endTime, int status) {
        this.jobIndexName = jobIndexName;
        this.jobId = jobId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
    }

    public StatusHistoryModel(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readInstant(), in.readOptionalInstant(), in.readInt());
    }

    public static StatusHistoryModel parse(final XContentParser parser) throws IOException {
        String jobIndexName = null;
        String jobId = null;
        Instant startTime = null;
        Instant endTime = null;
        Integer status = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case JOB_INDEX_NAME:
                    jobIndexName = parser.text();
                    break;
                case JOB_ID:
                    jobId = parser.text();
                    break;
                case START_TIME:
                    startTime = Instant.ofEpochSecond(parser.longValue());
                    break;
                case END_TIME:
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        endTime = Instant.ofEpochSecond(parser.longValue());
                    }
                    break;
                case COMPLETION_STATUS:
                    status = parser.intValue();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }

        return new StatusHistoryModel(
            requireNonNull(jobIndexName, "JobIndexName cannot be null"),
            requireNonNull(jobId, "JobId cannot be null"),
            requireNonNull(startTime, "startTime cannot be null"),
            endTime,
            requireNonNull(status, "status cannot be null")
        );
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject()
            .field(JOB_INDEX_NAME, this.jobIndexName)
            .field(JOB_ID, this.jobId)
            .field(START_TIME, this.startTime.getEpochSecond())
            .field(COMPLETION_STATUS, this.status);

        if (this.endTime != null) {
            builder.field(END_TIME, this.endTime.getEpochSecond());
        } else {
            builder.nullField(END_TIME);
        }

        return builder.endObject();
    }

    public String getJobIndexName() {
        return jobIndexName;
    }

    public String getJobId() {
        return jobId;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public int getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatusHistoryModel that = (StatusHistoryModel) o;
        return jobIndexName.equals(that.jobIndexName)
            && jobId.equals(that.jobId)
            && startTime.equals(that.startTime)
            && Objects.equals(endTime, that.endTime)
            && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIndexName, jobId, startTime, endTime, status);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.jobIndexName);
        out.writeString(this.jobId);
        out.writeInstant(this.startTime);
        out.writeOptionalInstant(this.endTime);
        out.writeInt(this.status);
    }
}
