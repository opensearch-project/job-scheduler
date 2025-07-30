/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.LockModel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetLocksResponse extends ActionResponse implements ToXContentObject {

    private Map<String, LockModel> locks;

    public GetLocksResponse() {
        this.locks = new HashMap<>();
    }

    public GetLocksResponse(Map<String, LockModel> locks) {
        this.locks = locks;
    }

    public GetLocksResponse(StreamInput in) throws IOException {
        super(in);
        this.locks = in.readMap(StreamInput::readString, LockModel::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(locks.size());
        out.writeMap(locks, StreamOutput::writeString, (stream, lock) -> lock.writeTo(stream));
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
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
