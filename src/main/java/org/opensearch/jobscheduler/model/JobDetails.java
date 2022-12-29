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

public class JobDetails implements ToXContentObject {

    private String jobIndex;

    private String jobType;

    private String jobParamAction;

    private String jobRunnerAction;

    public static final String JOB_INDEX = "job_index";
    public static final String JOB_TYPE = "job_type";
    public static final String JOB_PARAM_ACTION = "job_param_action";
    public static final String JOB_RUNNER_ACTION = "job_runner_action";

    public JobDetails() {}

    public JobDetails(String jobIndex, String jobType, String jobParamAction, String jobRunnerAction) {
        this.jobIndex = jobIndex;
        this.jobType = jobType;
        this.jobParamAction = jobParamAction;
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
        if (jobParamAction != null) {
            xContentBuilder.field(JOB_PARAM_ACTION, jobParamAction);
        }
        if (jobRunnerAction != null) {
            xContentBuilder.field(JOB_RUNNER_ACTION, jobRunnerAction);
        }
        return xContentBuilder.endObject();
    }

    public static JobDetails parse(XContentParser parser) throws IOException {
        String jobIndex = null;
        String jobType = null;
        String jobParamAction = null;
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
                case JOB_PARAM_ACTION:
                    jobParamAction = parser.text();
                    break;
                case JOB_RUNNER_ACTION:
                    jobRunnerAction = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new JobDetails(jobIndex, jobType, jobParamAction, jobRunnerAction);
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

    public String getJobParamAction() {
        return jobParamAction;
    }

    public void setJobParamAction(String jobParamAction) {
        this.jobParamAction = jobParamAction;
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
            && jobParamAction.equals(that.jobParamAction)
            && jobRunnerAction.equals(that.jobRunnerAction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIndex, jobType, jobParamAction, jobRunnerAction);
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
            + ", jobParamAction='"
            + jobParamAction
            + '\''
            + ", jobRunnerAction='"
            + jobRunnerAction
            + '\''
            + '}';
    }
}
