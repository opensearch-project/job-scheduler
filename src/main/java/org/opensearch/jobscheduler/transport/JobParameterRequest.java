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
 * Request to extensions to parse a ScheduledJobParameter
 */
public class JobParameterRequest implements Writeable {

    /**
     * jobSource is the index entry bytes reference from the registered job index
     */
    private final BytesReference jobSource;

    /**
     * id is the job Id
     */
    private final String id;

    /**
     * jobDocVersion is the metadata regarding this particular registered job
     */
    private final JobDocVersion jobDocVersion;

    /**
     * Instantiates a new Job Parameter Request
     *
     * @param jobParser the parser obect to extract the jobSource {@link BytesReference} from
     * @param id the job id
     * @param jobDocVersion the job document version
     * @throws IOException IOException when message de-serialization fails.
     */
    public JobParameterRequest(XContentParser jobParser, String id, JobDocVersion jobDocVersion) throws IOException {

        // Extract jobSource bytesRef from xContentParser
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.copyCurrentStructure(jobParser);

        this.jobSource = BytesReference.bytes(builder);
        this.id = id;
        this.jobDocVersion = jobDocVersion;
    }

    /**
     * Instantiates a new Job Parameter Request from {@link StreamInput}
     *
     * @param in in bytes stream input used to de-serialize the message.
     * @throws IOException IOException when message de-serialization fails.
     */
    public JobParameterRequest(StreamInput in) throws IOException {
        this.jobSource = in.readBytesReference();
        this.id = in.readString();
        this.jobDocVersion = new JobDocVersion(in);
    }

    /**
     * Instantiates a new Job Parameter Request by wrapping the given byte array within a {@link StreamInput}
     *
     * @param requestParams in bytes array used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public JobParameterRequest(byte[] requestParams) throws IOException {
        this(StreamInput.wrap(requestParams));
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
