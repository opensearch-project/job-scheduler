/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.model;

import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This model class stores the job details of the extension.
 */
public class JobDetails implements ToXContentObject {

    /**
     * jobIndex from the extension.
     */
    private String jobIndex;

    /**
     * jobType from the extension.
     */
    private String jobType;

    /**
     * jobParser action to trigger the response back to the extension.
     */
    private String jobParameterAction;

    /**
     * jobRunner action to trigger the response back to the extension.
     */
    private String jobRunnerAction;

    /**
     * extension unique ID
     */
    private String extensionUniqueId;

    public static final String DOCUMENT_ID = "document_id";
    public static final String JOB_INDEX = "job_index";
    public static final String JOB_TYPE = "job_type";
    public static final String JOB_PARAMETER_ACTION = "job_parser_action";
    public static final String JOB_RUNNER_ACTION = "job_runner_action";
    public static final String EXTENSION_UNIQUE_ID = "extension_unique_id";

    public JobDetails() {}

    public JobDetails(String jobIndex, String jobType, String jobParameterAction, String jobRunnerAction, String extensionUniqueId) {
        this.jobIndex = jobIndex;
        this.jobType = jobType;
        this.jobParameterAction = jobParameterAction;
        this.jobRunnerAction = jobRunnerAction;
        this.extensionUniqueId = extensionUniqueId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (jobIndex != null) {
            xContentBuilder.field(JOB_INDEX, jobIndex);
        }
        if (jobType != null) {
            xContentBuilder.field(JOB_TYPE, jobType);
        }
        if (jobParameterAction != null) {
            xContentBuilder.field(JOB_PARAMETER_ACTION, jobParameterAction);
        }
        if (jobRunnerAction != null) {
            xContentBuilder.field(JOB_RUNNER_ACTION, jobRunnerAction);
        }
        if (extensionUniqueId != null) {
            xContentBuilder.field(EXTENSION_UNIQUE_ID, extensionUniqueId);
        }
        return xContentBuilder.endObject();
    }

    public static JobDetails parse(XContentParser parser) throws IOException {
        String jobIndex = null;
        String jobType = null;
        String jobParameterAction = null;
        String jobRunnerAction = null;
        String extensionUniqueId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case JOB_INDEX:
                    jobIndex = parser.text();
                    break;
                case JOB_TYPE:
                    jobType = parser.text();
                    break;
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

        return new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
    }

    public JobDetails(final JobDetails copyJobDetails) {
        this(
            copyJobDetails.jobIndex,
            copyJobDetails.jobType,
            copyJobDetails.jobParameterAction,
            copyJobDetails.jobRunnerAction,
            copyJobDetails.extensionUniqueId
        );
    }

    @Nullable
    public String getJobIndex() {
        return jobIndex;
    }

    public void setJobIndex(String jobIndex) {
        this.jobIndex = jobIndex;
    }

    @Nullable
    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    @Nullable
    public String getJobParameterAction() {
        return jobParameterAction;
    }

    public void setJobParameterAction(String jobParameterAction) {
        this.jobParameterAction = jobParameterAction;
    }

    @Nullable
    public String getJobRunnerAction() {
        return jobRunnerAction;
    }

    public void setJobRunnerAction(String jobRunnerAction) {
        this.jobRunnerAction = jobRunnerAction;
    }

    @Nullable
    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

    public void setExtensionUniqueId(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDetails that = (JobDetails) o;
        return Objects.equals(jobIndex, that.jobIndex)
            && Objects.equals(jobType, that.jobType)
            && Objects.equals(jobParameterAction, that.jobParameterAction)
            && Objects.equals(jobRunnerAction, that.jobRunnerAction)
            && Objects.equals(extensionUniqueId, that.extensionUniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
    }

    @Override
    public String toString() {
        return "JobDetails{"
            + "jobIndex='"
            + jobIndex
            + '\''
            + ", jobType='"
            + jobType
            + '\''
            + ", jobParameterAction='"
            + jobParameterAction
            + '\''
            + ", jobRunnerAction='"
            + jobRunnerAction
            + '\''
            + ", extensionUniqueId='"
            + extensionUniqueId
            + '\''
            + '}';
    }
}
