/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

public class GetJobTypeRequest extends ActionRequest {

    private static String jobType;

    private static String extensionId;

    public static final String JOB_TYPE = "job_type";

    public static final String EXTENSION_ID = "extension_id";

    public GetJobTypeRequest(String jobType,String extensionId){
        super();
        this.jobType=jobType;
        this.extensionId=extensionId;
    }

    public GetJobTypeRequest(StreamInput in) throws IOException {
        super(in);
        jobType=in.readString();
        extensionId=in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jobType);
        out.writeString(extensionId);
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getExtensionId() {
        return extensionId;
    }

    public void setExtensionId(String extensionId) {
        this.extensionId = extensionId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static GetJobTypeRequest parse(
            XContentParser parser) throws IOException {

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case JOB_TYPE:
                    jobType = parser.text();
                    break;
                case EXTENSION_ID:
                    extensionId = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }

        }
        GetJobTypeRequest request = new GetJobTypeRequest(jobType,extensionId);
        return request;
    }
}
