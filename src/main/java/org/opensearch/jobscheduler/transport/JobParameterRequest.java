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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.jobscheduler.spi.JobDocVersion;

/**
 * Request to extensions to parse ScheduledJobParameter
 */
public class JobParameterRequest extends ExtensionActionRequest {

    public JobParameterRequest(String extensionActionName, XContentParser jobParser, String id, JobDocVersion jobDocVersion)
        throws IOException {
        super(extensionActionName, convertParamsToBytes(jobParser, id, jobDocVersion));
    }

    public static byte[] convertParamsToBytes(XContentParser jobParser, String id, JobDocVersion jobDocVersion) throws IOException {

        // Extract jobSource bytesRef from parser object
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.copyCurrentStructure(jobParser);
        BytesReference jobSource = BytesReference.bytes(builder);

        // write all to output stream
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeBytesReference(jobSource);
        out.writeString(id);
        jobDocVersion.writeTo(out);
        out.flush();

        // convert bytes stream to byte array
        return BytesReference.toBytes(out.bytes());
    }
}
