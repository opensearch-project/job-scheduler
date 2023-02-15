/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.request;

import java.util.Objects;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;

/**
 * Get Job Details Request Model class
 */
public class GetJobDetailsRequest extends ActionRequest {

    private static String documentId;

    private static String jobIndex;

    private static String jobType;

    private static String jobParameterAction;

    private static String jobRunnerAction;

    private static String extensionUniqueId;

    public static final String DOCUMENT_ID = "document_id";
    public static final String JOB_INDEX = "job_index";
    public static final String JOB_TYPE = "job_type";
    public static final String EXTENSION_UNIQUE_ID = "extension_unique_id";
    public static final String JOB_PARAMETER_ACTION = "job_parameter_action";
    public static final String JOB_RUNNER_ACTION = "job_runner_action";

    public GetJobDetailsRequest(StreamInput in) throws IOException {
        super(in);
        documentId = in.readOptionalString();
        jobIndex = in.readString();
        jobType = in.readString();
        jobParameterAction = in.readString();
        jobRunnerAction = in.readString();
        extensionUniqueId = in.readString();

    }

    public GetJobDetailsRequest(
        String documentId,
        String jobIndex,
        String jobType,
        String jobParameterAction,
        String jobRunnerAction,
        String extensionUniqueId
    ) {
        super();
        this.documentId = documentId;
        this.jobIndex = Objects.requireNonNull(jobIndex);
        this.jobType = Objects.requireNonNull(jobType);
        this.jobParameterAction = Objects.requireNonNull(jobParameterAction);
        this.jobRunnerAction = Objects.requireNonNull(jobRunnerAction);
        this.extensionUniqueId = Objects.requireNonNull(extensionUniqueId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(documentId);
        out.writeString(jobIndex);
        out.writeString(jobType);
        out.writeString(jobParameterAction);
        out.writeString(jobRunnerAction);
        out.writeString(extensionUniqueId);
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public String getJobIndex() {
        return jobIndex;
    }

    public void setJobIndex(String jobIndex) {
        this.jobIndex = jobIndex;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getJobParameterAction() {
        return jobParameterAction;
    }

    public void setJobParameterAction(String jobParameterAction) {
        this.jobParameterAction = jobParameterAction;
    }

    public String getJobRunnerAction() {
        return jobRunnerAction;
    }

    public void setJobRunnerAction(String jobRunnerAction) {
        this.jobRunnerAction = jobRunnerAction;
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

    public static GetJobDetailsRequest parse(XContentParser parser) throws IOException {

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case DOCUMENT_ID:
                    documentId = parser.textOrNull();
                    break;
                case JOB_INDEX:
                    jobIndex = parser.text();
                    break;
                case JOB_TYPE:
                    jobType = parser.text();
                case JOB_PARAMETER_ACTION:
                    jobParameterAction = parser.text();
                    break;
                case JOB_RUNNER_ACTION:
                    jobRunnerAction = parser.text();
                    break;
                case EXTENSION_UNIQUE_ID:
                    extensionUniqueId = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }

        }
        return new GetJobDetailsRequest(documentId, jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
    }
}
