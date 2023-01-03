/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.model;

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
    private String jobParserAction;

    /**
     * jobRunner action to trigger the response back to the extension.
     */
    private String jobRunnerAction;

    public static final String JOB_INDEX = "job_index";
    public static final String JOB_TYPE = "job_type";
    public static final String JOB_PARSER_ACTION = "job_parser_action";
    public static final String JOB_RUNNER_ACTION = "job_runner_action";

    public JobDetails() {}

    public JobDetails(String jobIndex, String jobType, String jobParserAction, String jobRunnerAction) {
        this.jobIndex = jobIndex;
        this.jobType = jobType;
        this.jobParserAction = jobParserAction;
        this.jobRunnerAction = jobRunnerAction;
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
        if (jobParserAction != null) {
            xContentBuilder.field(JOB_PARSER_ACTION, jobParserAction);
        }
        if (jobRunnerAction != null) {
            xContentBuilder.field(JOB_RUNNER_ACTION, jobRunnerAction);
        }
        return xContentBuilder.endObject();
    }

    public static JobDetails parse(XContentParser parser) throws IOException {
        String jobIndex = null;
        String jobType = null;
        String jobParserAction = null;
        String jobRunnerAction = null;

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
                case JOB_PARSER_ACTION:
                    jobParserAction = parser.text();
                    break;
                case JOB_RUNNER_ACTION:
                    jobRunnerAction = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new JobDetails(jobIndex, jobType, jobParserAction, jobRunnerAction);
    }

    public JobDetails(final JobDetails copyJobDetails) {
        this(copyJobDetails.jobIndex, copyJobDetails.jobType, copyJobDetails.jobParserAction, copyJobDetails.jobRunnerAction);
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

    public String getJobParserAction() {
        return jobParserAction;
    }

    public void setJobParserAction(String jobParserAction) {
        this.jobParserAction = jobParserAction;
    }

    public String getJobRunnerAction() {
        return jobRunnerAction;
    }

    public void setJobRunnerAction(String jobRunnerAction) {
        this.jobRunnerAction = jobRunnerAction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDetails that = (JobDetails) o;
        return Objects.equals(jobIndex, that.jobIndex)
            && Objects.equals(jobType, that.jobType)
            && Objects.equals(jobParserAction, that.jobParserAction)
            && Objects.equals(jobRunnerAction, that.jobRunnerAction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIndex, jobType, jobParserAction, jobRunnerAction);
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
            + ", jobParserAction='"
            + jobParserAction
            + '\''
            + ", jobRunnerAction='"
            + jobRunnerAction
            + '\''
            + '}';
    }
}
