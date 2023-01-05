/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;

/**
 * Get Job Index Request Model class
 */
public class GetJobIndexRequest extends ActionRequest {

    private static String jobIndex;

    private static String jobParameterAction;

    private static String jobRunnerAction;

    private static String extensionId;

    public static final String JOB_INDEX = "job_index";

    public static final String EXTENSION_ID = "extension_id";
    private static final String JOB_PARAMETER_ACTION = "job_parameter_action";
    public static final String JOB_RUNNER_ACTION = "job_runner_action";

    public GetJobIndexRequest(StreamInput in) throws IOException {
        super(in);
        jobIndex = in.readString();
        jobParameterAction = in.readString();
        jobRunnerAction = in.readString();
        extensionId = in.readString();

    }

    public GetJobIndexRequest(String jobIndex, String jobParameterAction, String jobRunnerAction, String extensionId) {
        super();
        this.jobIndex = jobIndex;
        this.jobParameterAction = jobParameterAction;
        this.jobRunnerAction = jobRunnerAction;
        this.extensionId = extensionId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jobIndex);
        out.writeString(jobParameterAction);
        out.writeString(jobRunnerAction);
        out.writeString(extensionId);
    }

    public String getJobIndex() {
        return jobIndex;
    }

    public void setJobIndex(String jobIndex) {
        this.jobIndex = jobIndex;
    }

    public static String getJobParameterAction() {
        return jobParameterAction;
    }

    public static void setJobParameterAction(String jobParameterAction) {
        GetJobIndexRequest.jobParameterAction = jobParameterAction;
    }

    public String getJobRunnerAction() {
        return jobRunnerAction;
    }

    public void setJobRunnerAction(String jobRunnerAction) {
        this.jobRunnerAction = jobRunnerAction;
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

    public static GetJobIndexRequest parse(XContentParser parser) throws IOException {

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case JOB_INDEX:
                    jobIndex = parser.text();
                    break;
                case JOB_PARAMETER_ACTION:
                    jobParameterAction = parser.text();
                    break;
                case JOB_RUNNER_ACTION:
                    jobRunnerAction = parser.text();
                    break;
                case EXTENSION_ID:
                    extensionId = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }

        }
        return new GetJobIndexRequest(jobIndex, jobParameterAction, jobRunnerAction, extensionId);
    }
}
