/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.LockModel;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class GetLocksResponse extends ActionResponse implements ToXContentObject {

    private Map<String, LockModel> locks;
    private static final DateFormatter STRICT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time");

    public GetLocksResponse() {
        this.locks = new HashMap<>();
    }

    public GetLocksResponse(Map<String, LockModel> locks) {
        this.locks = locks;
    }

    public GetLocksResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readInt();
        this.locks = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            // Read LockModel fields
            String jobIndexName = in.readString();
            String jobId = in.readString();
            long lockTimeSeconds = in.readLong();
            long lockDurationSeconds = in.readLong();
            boolean released = in.readBoolean();
            long seqNo = in.readLong();
            long primaryTerm = in.readLong();

            // Create LockModel
            LockModel value = new LockModel(
                jobIndexName,
                jobId,
                Instant.ofEpochSecond(lockTimeSeconds),
                lockDurationSeconds,
                released,
                seqNo,
                primaryTerm
            );
            this.locks.put(key, value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(locks.size());
        for (Map.Entry<String, LockModel> entry : locks.entrySet()) {
            LockModel lock = entry.getValue();
            out.writeString(entry.getKey());
            // Write LockModel fields
            out.writeString(lock.getJobIndexName());
            out.writeString(lock.getJobId());
            out.writeLong(lock.getLockTime().getEpochSecond());
            out.writeLong(lock.getLockDurationSeconds());
            out.writeBoolean(lock.isReleased());
            out.writeLong(lock.getSeqNo());
            out.writeLong(lock.getPrimaryTerm());
        }
    }

    public Map<String, LockModel> getLocks() {
        return locks;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("total_locks", locks.size());
        builder.startObject("locks");
        for (Map.Entry<String, LockModel> entry : locks.entrySet()) {
            LockModel lock = entry.getValue();
            builder.startObject(entry.getKey())
                .field("job_index_name", lock.getJobIndexName())
                .field("job_id", lock.getJobId())
                .field("lock_aquired_time", STRICT_DATE_TIME_FORMATTER.format(lock.getLockTime().atOffset(ZoneOffset.UTC)))
                .field("lock_duration_seconds", lock.getLockDurationSeconds())
                .field("released", lock.isReleased())
                .endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
