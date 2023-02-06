/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

public class AcquireLockRequest extends ActionRequest {

    private final String jobId;

    private final String jobIndexName;

    private final long lockDurationSeconds;

    public static final String JOB_ID = "job_id";
    public static final String JOB_INDEX_NAME = "job_index_name";
    public static final String LOCK_DURATION_SECONDS = "lock_duration_seconds";

    public AcquireLockRequest(StreamInput in) throws IOException {
        super(in);
        this.jobId = in.readString();
        this.jobIndexName = in.readString();
        this.lockDurationSeconds = in.readLong();
    }

    public AcquireLockRequest(String jobId, String jobIndexName, long lockDurationSeconds) {
        super();
        this.jobId = Objects.requireNonNull(jobId);
        this.jobIndexName = Objects.requireNonNull(jobIndexName);
        this.lockDurationSeconds = Objects.requireNonNull(lockDurationSeconds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.jobId);
        out.writeString(this.jobIndexName);
        out.writeLong(this.lockDurationSeconds);
    }

    public String getJobId() {
        return this.jobId;
    }

    public String getJobIndexName() {
        return this.jobIndexName;
    }

    public long getLockDurationSeconds() {
        return this.lockDurationSeconds;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static AcquireLockRequest parse(XContentParser parser) throws IOException {

        String jobId = null;
        String jobIndexName = null;
        Long lockDurationSeconds = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case JOB_ID:
                    jobId = parser.text();
                    break;
                case JOB_INDEX_NAME:
                    jobIndexName = parser.text();
                    break;
                case LOCK_DURATION_SECONDS:
                    lockDurationSeconds = parser.longValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AcquireLockRequest(jobId, jobIndexName, lockDurationSeconds);
    }

}
