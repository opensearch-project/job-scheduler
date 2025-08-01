/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class LockModel implements ToXContentObject, Writeable {
    private static final String LOCK_ID_DELIMITER = "-";
    public static final String JOB_INDEX_NAME = "job_index_name";
    public static final String JOB_ID = "job_id";
    public static final String LOCK_TIME = "lock_time";
    public static final String LOCK_DURATION = "lock_duration_seconds";
    public static final String RELEASED = "released";

    // Rest Fields
    public static final String GET_LOCK_ACTION = "get_lock_action";
    public static final String SEQUENCE_NUMBER = "seq_no";
    public static final String PRIMARY_TERM = "primary_term";
    public static final String LOCK_ID = "lock_id";
    public static final String LOCK_MODEL = "lock_model";

    private final String lockId;
    private final String jobIndexName;
    private final String jobId;
    private final Instant lockTime;
    private final long lockDurationSeconds;
    private final boolean released;
    private final long seqNo;
    private final long primaryTerm;

    /**
     * Use this constructor to copy existing lock and update the seqNo and primaryTerm.
     *
     * @param copyLock    JobSchedulerLockModel to copy from.
     * @param seqNo       sequence number from OpenSearch document.
     * @param primaryTerm primary term from OpenSearch document.
     */
    public LockModel(final LockModel copyLock, long seqNo, long primaryTerm) {
        this(copyLock.jobIndexName, copyLock.jobId, copyLock.lockTime, copyLock.lockDurationSeconds, copyLock.released, seqNo, primaryTerm);
    }

    /**
     * Use this constructor to copy existing lock and change status of the released of the lock.
     *
     * @param copyLock JobSchedulerLockModel to copy from.
     * @param released boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock, final boolean released) {
        this(
            copyLock.jobIndexName,
            copyLock.jobId,
            copyLock.lockTime,
            copyLock.lockDurationSeconds,
            released,
            copyLock.seqNo,
            copyLock.primaryTerm
        );
    }

    /**
     * Use this constructor to copy existing lock and change the duration of the lock.
     *
     * @param copyLock            JobSchedulerLockModel to copy from.
     * @param updateLockTime      new updated lock time to start the lock.
     * @param lockDurationSeconds total lock duration in seconds.
     * @param released            boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock, final Instant updateLockTime, final long lockDurationSeconds, final boolean released) {
        this(copyLock.jobIndexName, copyLock.jobId, updateLockTime, lockDurationSeconds, released, copyLock.seqNo, copyLock.primaryTerm);
    }

    public LockModel(String jobIndexName, String jobId, Instant lockTime, long lockDurationSeconds, boolean released) {
        this(
            jobIndexName,
            jobId,
            lockTime,
            lockDurationSeconds,
            released,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
    }

    public LockModel(
        String jobIndexName,
        String jobId,
        Instant lockTime,
        long lockDurationSeconds,
        boolean released,
        long seqNo,
        long primaryTerm
    ) {
        this.lockId = jobIndexName + LOCK_ID_DELIMITER + jobId;
        this.jobIndexName = jobIndexName;
        // The jobId parameter does not necessarily need to represent the id of a job scheduler job, as it is being used
        // to scope the lock, and could represent any resource.
        this.jobId = jobId;
        this.lockTime = lockTime;
        this.lockDurationSeconds = lockDurationSeconds;
        this.released = released;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    public LockModel(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readInstant(),
            in.readLong(),
            in.readBoolean(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
    }

    public static String generateLockId(String jobIndexName, String jobId) {
        return jobIndexName + LOCK_ID_DELIMITER + jobId;
    }

    public static LockModel parse(final XContentParser parser, long seqNo, long primaryTerm) throws IOException {
        String jobIndexName = null;
        String jobId = null;
        Instant lockTime = null;
        Long lockDurationSecond = null;
        Boolean released = null;

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
                case LOCK_TIME:
                    lockTime = Instant.ofEpochSecond(parser.longValue());
                    break;
                case LOCK_DURATION:
                    lockDurationSecond = parser.longValue();
                    break;
                case RELEASED:
                    released = parser.booleanValue();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }

        return new LockModel(
            requireNonNull(jobIndexName, "JobIndexName cannot be null"),
            requireNonNull(jobId, "JobId cannot be null"),
            requireNonNull(lockTime, "lockTime cannot be null"),
            requireNonNull(lockDurationSecond, "lockDurationSeconds cannot be null"),
            requireNonNull(released, "released cannot be null"),
            seqNo,
            primaryTerm
        );
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject()
            .field(JOB_INDEX_NAME, this.jobIndexName)
            .field(JOB_ID, this.jobId)
            .field(LOCK_TIME, this.lockTime.getEpochSecond())
            .field(LOCK_DURATION, this.lockDurationSeconds)
            .field(RELEASED, this.released)
            .endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.humanReadable(true);
            this.toXContent(builder, EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            try {
                XContentBuilder builder = JsonXContent.contentBuilder();
                builder.startObject();
                builder.field("error", "error building toString out of XContent: " + e.getMessage());
                builder.field("stack_trace", ExceptionsHelper.stackTrace(e));
                builder.endObject();
                return BytesReference.bytes(builder).utf8ToString();
            } catch (IOException e2) {
                throw new OpenSearchException("cannot generate error message for deserialization", e);
            }
        }
    }

    public String getLockId() {
        return lockId;
    }

    public String getJobIndexName() {
        return jobIndexName;
    }

    public String getJobId() {
        return jobId;
    }

    public Instant getLockTime() {
        return lockTime;
    }

    public long getLockDurationSeconds() {
        return lockDurationSeconds;
    }

    public boolean isReleased() {
        return released;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public boolean isExpired() {
        return lockTime.getEpochSecond() + lockDurationSeconds < Instant.now().getEpochSecond();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockModel lockModel = (LockModel) o;
        return lockDurationSeconds == lockModel.lockDurationSeconds
            && released == lockModel.released
            && seqNo == lockModel.seqNo
            && primaryTerm == lockModel.primaryTerm
            && lockId.equals(lockModel.lockId)
            && jobIndexName.equals(lockModel.jobIndexName)
            && jobId.equals(lockModel.jobId)
            && lockTime.equals(lockModel.lockTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockId, jobIndexName, jobId, lockTime, lockDurationSeconds, released, seqNo, primaryTerm);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write LockModel fields
        out.writeString(this.jobIndexName);
        out.writeString(this.jobId);
        out.writeInstant(this.lockTime);
        out.writeLong(this.lockDurationSeconds);
        out.writeBoolean(this.released);
        out.writeLong(this.seqNo);
        out.writeLong(this.primaryTerm);
    }
}
