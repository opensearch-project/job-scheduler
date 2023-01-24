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
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.JobDocVersion;

/**
 * Request to extensions to parse ScheduledJobParameter
 */
public class JobParameterRequest implements Writeable {

    private final BytesReference jobSource;

    private final String id;

    private final JobDocVersion jobDocVersion;

    public JobParameterRequest(XContentParser jobParser, String id, JobDocVersion jobDocVersion) throws IOException {

        // Extract jobSource bytesRef from xContentParser
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.copyCurrentStructure(jobParser);

        this.jobSource = BytesReference.bytes(builder);
        this.id = id;
        this.jobDocVersion = jobDocVersion;
    }

    public JobParameterRequest(StreamInput in) throws IOException {
        this.jobSource = in.readBytesReference();
        this.id = in.readString();
        this.jobDocVersion = new JobDocVersion(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(this.jobSource);
        out.writeString(this.id);
        this.jobDocVersion.writeTo(out);
    }

    public BytesReference getJobSource() {
        return this.jobSource;
    }

    public String getId() {
        return this.id;
    }

    public JobDocVersion getJobDocVersion() {
        return this.jobDocVersion;
    }
}
