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
import org.opensearch.jobscheduler.spi.StatusHistoryModel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetHistoryResponse extends ActionResponse implements ToXContentObject {

    private Map<String, StatusHistoryModel> history;

    public GetHistoryResponse() {
        this.history = new HashMap<>();
    }

    public GetHistoryResponse(Map<String, StatusHistoryModel> history) {
        this.history = history;
    }

    public GetHistoryResponse(StreamInput in) throws IOException {
        super(in);
        this.history = in.readMap(StreamInput::readString, StatusHistoryModel::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(history.size());
        out.writeMap(history, StreamOutput::writeString, (stream, historyModel) -> historyModel.writeTo(stream));
    }

    public Map<String, StatusHistoryModel> getHistory() {
        return history;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("total_history", history.size());
        builder.startObject("history");
        for (Map.Entry<String, StatusHistoryModel> entry : history.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
