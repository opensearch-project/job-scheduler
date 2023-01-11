/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import java.util.Objects;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Get Job Type Request Model class
 */
public class GetJobTypeRequest extends ActionRequest {

    private static String jobType;

    private static String extensionUniqueId;

    public static final String JOB_TYPE = "job_type";

    public static final String EXTENSION_UNIQUE_ID = "extension_unique_id";

    public GetJobTypeRequest(String jobType, String extensionUniqueId) {
        super();
        this.jobType = Objects.requireNonNull(jobType);
        this.extensionUniqueId = Objects.requireNonNull(extensionUniqueId);
    }

    public GetJobTypeRequest(StreamInput in) throws IOException {
        super(in);
        jobType = in.readString();
        extensionUniqueId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jobType);
        out.writeString(extensionUniqueId);
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

    public void setExtensionUniqueId(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static GetJobTypeRequest parse(XContentParser parser) throws IOException {

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case JOB_TYPE:
                    jobType = parser.text();
                    break;
                case EXTENSION_UNIQUE_ID:
                    extensionUniqueId = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }

        }
        return new GetJobTypeRequest(jobType, extensionUniqueId);
    }
}
