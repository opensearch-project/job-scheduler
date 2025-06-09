/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.request;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

import java.io.IOException;

public class GetAllJobInfoRequest extends ActionRequest {

    private boolean activeJobsOnly;
    
    public static final String ACTIVE_JOBS_ONLY = "active_jobs_only";
    //public static final String ALL_SCHEDULED_JOBS = "all_scheduled_jobs";

    public GetAllJobInfoRequest(StreamInput in) throws IOException {
        super(in);
        this.activeJobsOnly = in.readBoolean();
    }

    public GetAllJobInfoRequest(boolean activeJobsOnly) {
        super();
        this.activeJobsOnly = activeJobsOnly;
    }

    public void setActiveJobsOnly(boolean activeJobsOnly) {
        this.activeJobsOnly = activeJobsOnly;
    }

    public boolean isActiveJobsOnly() {
        return activeJobsOnly;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(activeJobsOnly);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static GetAllJobInfoRequest parse(XContentParser parser) throws IOException {
        boolean activeJobsOnly = true; // Default to all schedule jobs

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ACTIVE_JOBS_ONLY:
                    activeJobsOnly = parser.booleanValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new GetAllJobInfoRequest(activeJobsOnly);
    }

}