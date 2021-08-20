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
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.jobscheduler.spi;

import org.opensearch.common.Strings;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class LockModel implements ToXContentObject {
    private static final String LOCK_ID_DELIMITER = "-";
    public static final String JOB_INDEX_NAME = "job_index_name";
    public static final String JOB_ID = "job_id";
    public static final String LOCK_TIME = "lock_time";
    public static final String LOCK_DURATION = "lock_duration_seconds";
    public static final String RELEASED = "released";
    public static final String RESOURCE = "resource";
    public static final String RESOURCE_TYPE = "resource_type";

    private final String lockId;
    private final String jobIndexName;
    private final String jobId;
    private final Instant lockTime;
    private final long lockDurationSeconds;
    private final boolean released;
    private final long seqNo;
    private final long primaryTerm;
    private final Map<String, Object> resource;
    private final String resourceType;

    /**
     * Use this constructor to copy existing lock and update the seqNo and primaryTerm.
     *
     * @param copyLock    JobSchedulerLockModel to copy from.
     * @param seqNo       sequence number from OpenSearch document.
     * @param primaryTerm primary term from OpenSearch document.
     */
    public LockModel(final LockModel copyLock, long seqNo, long primaryTerm) throws IOException {
        this (
            copyLock.jobIndexName,
            copyLock.jobId,
            copyLock.resourceType,
            copyLock.resource,
            copyLock.lockTime,
            copyLock.lockDurationSeconds,
            copyLock.released,
            seqNo,
            primaryTerm
        );
    }

    /**
     * Use this constructor to copy existing lock and change status of the released of the lock.
     *
     * @param copyLock JobSchedulerLockModel to copy from.
     * @param released boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock, final boolean released) throws IOException {
        this (
            copyLock.jobIndexName,
            copyLock.jobId,
            copyLock.resourceType,
            copyLock.resource,
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
    public LockModel(final LockModel copyLock,
                     final Instant updateLockTime, final long lockDurationSeconds, final boolean released) throws IOException {
        this (
            copyLock.jobIndexName,
            copyLock.jobId,
            copyLock.resourceType,
            copyLock.resource,
            updateLockTime,
            lockDurationSeconds,
            released,
            copyLock.seqNo,
            copyLock.primaryTerm
        );
    }

    /**
     * Use this constructor to build a job-scheduler lock.
     * @param jobIndexName          index of job acquiring the lock.
     * @param jobId                 id of job acquiring the lock.
     * @param lockTime              time at which the lock is acquired.
     * @param lockDurationSeconds   duration for which the lock is valid.
     * @param released              boolean flag to indicate if the lock is released.
     */
    public LockModel(String jobIndexName, String jobId, Instant lockTime, long lockDurationSeconds, boolean released) throws IOException {
        this(jobIndexName, jobId, lockTime, lockDurationSeconds, released,
                SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    }

    /**
     * Fully parametrized constructor for building a job-scheduler lock
     * @param jobIndexName          index of job acquiring the lock.
     * @param jobId                 id of job acquiring the lock.
     * @param lockTime              time at which the lock is acquired.
     * @param lockDurationSeconds   duration for which the lock is valid.
     * @param released              boolean flag to indicate if the lock is released.
     * @param seqNo                 sequence number from OpenSearch document.
     * @param primaryTerm           primary term from OpenSearch document.
     */
    public LockModel(String jobIndexName, String jobId, Instant lockTime,
                     long lockDurationSeconds, boolean released, long seqNo, long primaryTerm) throws IOException {
        this(jobIndexName, jobId, null, null, lockTime, lockDurationSeconds, released, seqNo, primaryTerm);
    }

    public LockModel(String jobIndexName, String jobId, String resourceType, Map<String, Object> resource, Instant lockTime,
                     long lockDurationSeconds, boolean released) throws IOException {
        this(jobIndexName, jobId, resourceType, resource, lockTime, lockDurationSeconds, released,
                SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    }

    /***
     *
     * @param jobIndexName          index of job acquiring the lock
     * @param jobId                 id of the job acquiring the lock
     * @param resourceType          type of job acquiring the lock
     * @param resource              resource to acquire
     * @param lockTime              time at which lock is acquired
     * @param lockDurationSeconds   duration for which lock is valid
     * @param released              boolean flag for whether lock is released
     * @param seqNo                 sequence number from Opensearch document
     * @param primaryTerm           primary term from Opensearch document
     * @throws IOException          throws IOException if resource fails to serialize
     */
    public LockModel(String jobIndexName, String jobId, String resourceType, Map<String, Object> resource, Instant lockTime,
                     long lockDurationSeconds, boolean released, long seqNo, long primaryTerm) throws IOException {
        if (resourceType != null) {
            this.lockId = LockModel.generateResourceLockId(jobIndexName, resourceType, resource);
            this.jobIndexName = jobIndexName;
            this.jobId = jobId;
            this.resourceType = resourceType;
            this.resource = resource;
            this.lockTime = lockTime;
            this.lockDurationSeconds = lockDurationSeconds;
            this.released = released;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
        }
        else {
            this.lockId = jobIndexName + LOCK_ID_DELIMITER + jobId;
            this.jobIndexName = jobIndexName;
            this.jobId = jobId;
            this.lockTime = lockTime;
            this.lockDurationSeconds = lockDurationSeconds;
            this.released = released;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.resource = null;
            this.resourceType = null;
        }

    }

    public static String generateLockId(String jobIndexName, String jobId) {
        return jobIndexName + LOCK_ID_DELIMITER + jobId;
    }

    public static String generateResourceLockId(String jobIndexName, String resourceType, Map<String, Object> resource) throws IOException {
        try {
            byte[] resourceAsBytes = serialize(resource);
            MurmurHash3.Hash128 hash = MurmurHash3.hash128(resourceAsBytes, 0, resourceAsBytes.length, 0,
                    new MurmurHash3.Hash128());
            byte[] resourceHashBytes = ByteBuffer.allocate(16).putLong(hash.h1).putLong(hash.h2).array();
            String resourceAsHashString = Base64.getUrlEncoder().withoutPadding().encodeToString(resourceHashBytes);
            return jobIndexName + LOCK_ID_DELIMITER + resourceType + LOCK_ID_DELIMITER + resourceAsHashString;
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw ioException;
        }
    }

    private static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    public static LockModel parse(final XContentParser parser, long seqNo, long primaryTerm) throws IOException {
        String jobIndexName = null;
        String jobId = null;
        String resourceType = null;
        Instant lockTime = null;
        Long lockDurationSecond = null;
        Boolean released = null;
        Map<String, Object> resource = null;

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
                case RESOURCE:
                    resource = parser.map();
                    break;
                case RESOURCE_TYPE:
                    resourceType = parser.text();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }

        return new LockModel(
                requireNonNull(jobIndexName, "JobIndexName cannot be null"),
                requireNonNull(jobId, "JobId cannot be null"),
                resourceType,
                resource,
                requireNonNull(lockTime, "lockTime cannot be null"),
                requireNonNull(lockDurationSecond, "lockDurationSeconds cannot be null"),
                requireNonNull(released, "released cannot be null"),
                seqNo,
                primaryTerm
        );
    }

    @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject()
                .field(JOB_INDEX_NAME, this.jobIndexName)
                .field(JOB_ID, this.jobId)
                .field(LOCK_TIME, this.lockTime.getEpochSecond())
                .field(LOCK_DURATION, this.lockDurationSeconds)
                .field(RELEASED, this.released);
        if (resource != null) {
            builder.field(RESOURCE, this.resource)
                    .field(RESOURCE_TYPE, this.resourceType);
        }
        return builder.endObject();
    }

    @Override public String toString() {
        return Strings.toString(this, false, true);
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

    public Map<String, Object> getResource() {
        return resource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockModel lockModel = (LockModel) o;
        boolean resourceCheck = Objects.equals(resource, lockModel.resource);
        boolean resourceTypeCheck = Objects.equals(resourceType, lockModel.resourceType);
        return lockDurationSeconds == lockModel.lockDurationSeconds &&
                released == lockModel.released &&
                seqNo == lockModel.seqNo &&
                primaryTerm == lockModel.primaryTerm &&
                lockId.equals(lockModel.lockId) &&
                jobIndexName.equals(lockModel.jobIndexName) &&
                jobId.equals(lockModel.jobId) &&
                lockTime.equals(lockModel.lockTime) &&
                resourceCheck &&
                resourceTypeCheck;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockId, jobIndexName, jobId, lockTime, lockDurationSeconds, released, seqNo, primaryTerm);
    }
}
